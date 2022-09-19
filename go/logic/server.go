/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"text/tabwriter"

	"github.com/github/gh-ost/go/base"
)

const throttleHint = "# Note: you may only throttle for as long as your binary logs are not purged\n"

type PrintStatusFunc func(PrintStatusRule, io.Writer)

// Server listens for requests on a socket file or via TCP
type Server struct {
	appVersion       string
	migrationContext *base.MigrationContext
	unixListener     net.Listener
	tcpListener      net.Listener
	hooksExecutor    *HooksExecutor
	printStatus      PrintStatusFunc
}

func NewServer(migrationContext *base.MigrationContext, hooksExecutor *HooksExecutor, printStatus PrintStatusFunc, appVersion string) *Server {
	return &Server{
		appVersion:       appVersion,
		migrationContext: migrationContext,
		hooksExecutor:    hooksExecutor,
		printStatus:      printStatus,
	}
}

func (this *Server) BindSocketFile() (err error) {
	if this.migrationContext.ServeSocketFile == "" {
		return nil
	}
	if this.migrationContext.DropServeSocket && base.FileExists(this.migrationContext.ServeSocketFile) {
		os.Remove(this.migrationContext.ServeSocketFile)
	}
	this.unixListener, err = net.Listen("unix", this.migrationContext.ServeSocketFile)
	if err != nil {
		return err
	}
	this.migrationContext.Log.Infof("Listening on unix socket file: %s", this.migrationContext.ServeSocketFile)
	return nil
}

func (this *Server) RemoveSocketFile() (err error) {
	this.migrationContext.Log.Infof("Removing socket file: %s", this.migrationContext.ServeSocketFile)
	return os.Remove(this.migrationContext.ServeSocketFile)
}

func (this *Server) BindTCPPort() (err error) {
	if this.migrationContext.ServeTCPPort == 0 {
		return nil
	}
	this.tcpListener, err = net.Listen("tcp", fmt.Sprintf(":%d", this.migrationContext.ServeTCPPort))
	if err != nil {
		return err
	}
	this.migrationContext.Log.Infof("Listening on tcp port: %d", this.migrationContext.ServeTCPPort)
	return nil
}

// Serve begins listening & serving on whichever device was configured
func (this *Server) Serve() (err error) {
	go func() {
		for {
			conn, err := this.unixListener.Accept()
			if err != nil {
				this.migrationContext.Log.Errore(err)
			}
			go this.handleConnection(conn)
		}
	}()
	go func() {
		if this.tcpListener == nil {
			return
		}
		for {
			conn, err := this.tcpListener.Accept()
			if err != nil {
				this.migrationContext.Log.Errore(err)
			}
			go this.handleConnection(conn)
		}
	}()

	return nil
}

func (this *Server) handleConnection(conn net.Conn) (err error) {
	if conn != nil {
		defer conn.Close()
	}
	command, _, err := bufio.NewReader(conn).ReadLine()
	if err != nil {
		return err
	}
	return this.onServerCommand(string(command), bufio.NewWriter(conn))
}

// onServerCommand responds to a user's interactive command
func (this *Server) onServerCommand(command string, writer *bufio.Writer) (err error) {
	defer writer.Flush()

	printStatusRule, err := this.applyServerCommand(command, writer)
	if err == nil {
		this.printStatus(printStatusRule, writer)
	} else {
		fmt.Fprintf(writer, "%s\n", err.Error())
	}
	return this.migrationContext.Log.Errore(err)
}

type ValueType string

var (
	StringValue  ValueType = "string"
	Int64Value   ValueType = "integer"
	Float64Value ValueType = "float"
)

// ServerCommand represents a server command/action
type ServerCommand struct {
	Name      string
	Aliases   []string
	Help      string
	ValueType ValueType
	ValueHelp string
	Action    func(writer io.Writer, this *Server, arg string) (PrintStatusRule, error)
}

func isArgQuestion(arg string) bool {
	return (arg == "?")
}

// ServerCommands represents all available server commands
var ServerCommands = []ServerCommand{
	{
		Name: "help",
		Help: "Print this message",
	},
	{
		Name: "sup",
		Help: "Print a short status message",
		Action: func(writer io.Writer, this *Server, arg string) (PrintStatusRule, error) {
			return ForcePrintStatusOnlyRule, nil
		},
	},
	{
		Name: "version",
		Help: "Print the gh-ost version",
		Action: func(writer io.Writer, this *Server, arg string) (PrintStatusRule, error) {
			fmt.Fprintf(writer, "gh-ost version: %s\n", this.appVersion)
			return ForcePrintStatusAndHintRule, nil
		},
	},
	{
		Name:    "status",
		Aliases: []string{"info"},
		Help:    "Print a detailed status message",
		Action: func(writer io.Writer, this *Server, arg string) (PrintStatusRule, error) {
			return ForcePrintStatusAndHintRule, nil
		},
	},
	{
		Name: "coordinates",
		Help: "Print the currently inspected coordinates",
		Action: func(writer io.Writer, this *Server, arg string) (PrintStatusRule, error) {
			fmt.Fprintf(writer, "%+v\n", this.migrationContext.GetRecentBinlogCoordinates())
			return NoPrintStatusRule, nil
		},
	},
	{
		Name: "applier",
		Help: "Print the hostname of the applier",
		Action: func(writer io.Writer, this *Server, arg string) (PrintStatusRule, error) {
			if this.migrationContext.ApplierConnectionConfig != nil && this.migrationContext.ApplierConnectionConfig.ImpliedKey != nil {
				fmt.Fprintf(writer, "Host: %s, Version: %s\n",
					this.migrationContext.ApplierConnectionConfig.ImpliedKey.String(),
					this.migrationContext.ApplierMySQLVersion,
				)
			}
			return NoPrintStatusRule, nil
		},
	},
	{
		Name: "inspector",
		Help: "Print the hostname of the inspector",
		Action: func(writer io.Writer, this *Server, arg string) (PrintStatusRule, error) {
			if this.migrationContext.InspectorConnectionConfig != nil && this.migrationContext.InspectorConnectionConfig.ImpliedKey != nil {
				fmt.Fprintf(writer, "Host: %s, Version: %s\n",
					this.migrationContext.InspectorConnectionConfig.ImpliedKey.String(),
					this.migrationContext.InspectorMySQLVersion,
				)
			}
			return NoPrintStatusRule, nil
		},
	},
	{
		Name:      "chunk-size",
		Help:      "Set a new chunk-size",
		ValueType: Int64Value,
		ValueHelp: "newsize",
		Action: func(writer io.Writer, this *Server, arg string) (PrintStatusRule, error) {
			if isArgQuestion(arg) {
				fmt.Fprintf(writer, "%+v\n", atomic.LoadInt64(&this.migrationContext.ChunkSize))
				return NoPrintStatusRule, nil
			}
			if chunkSize, err := strconv.ParseInt(arg, 10, 64); err != nil {
				return NoPrintStatusRule, err
			} else {
				this.migrationContext.SetChunkSize(chunkSize)
				return ForcePrintStatusAndHintRule, nil
			}
		},
	},
	{
		Name:      "dml-batch-size",
		Help:      "Set a new dml-batch-size",
		ValueType: Int64Value,
		ValueHelp: "newsize",
		Action: func(writer io.Writer, this *Server, arg string) (PrintStatusRule, error) {
			if isArgQuestion(arg) {
				fmt.Fprintf(writer, "%+v\n", atomic.LoadInt64(&this.migrationContext.DMLBatchSize))
				return NoPrintStatusRule, nil
			}
			if dmlBatchSize, err := strconv.ParseInt(arg, 10, 64); err != nil {
				return NoPrintStatusRule, err
			} else {
				this.migrationContext.SetDMLBatchSize(dmlBatchSize)
				return ForcePrintStatusAndHintRule, nil
			}
		},
	},
	{
		Name:      "max-lag-millis",
		Help:      "Set a new replication lag threshold",
		ValueType: Int64Value,
		ValueHelp: "max-lag",
		Action: func(writer io.Writer, this *Server, arg string) (PrintStatusRule, error) {
			if isArgQuestion(arg) {
				fmt.Fprintf(writer, "%+v\n", atomic.LoadInt64(&this.migrationContext.MaxLagMillisecondsThrottleThreshold))
				return NoPrintStatusRule, nil
			}
			if maxLagMillis, err := strconv.ParseInt(arg, 10, 64); err != nil {
				return NoPrintStatusRule, err
			} else {
				this.migrationContext.SetMaxLagMillisecondsThrottleThreshold(maxLagMillis)
				return ForcePrintStatusAndHintRule, nil
			}
		},
	},
	{
		Name:      "replication-lag-query",
		Help:      "(Deprecated) set a new query that determines replication lag without quotes",
		ValueType: StringValue,
		ValueHelp: "query",
		Action: func(writer io.Writer, this *Server, arg string) (PrintStatusRule, error) {
			return NoPrintStatusRule, fmt.Errorf("replication-lag-query is deprecated. gh-ost uses an internal, subsecond resolution query")
		},
	},
	{
		Name:      "nice-ratio",
		Help:      "Set a new nice-ratio, immediate sleep after each row-copy operation, float (examples: 0 is aggressive, 0.7 adds 70% runtime, 1.0 doubles runtime, 2.0 triples runtime, ...)",
		ValueType: Float64Value,
		ValueHelp: "ratio",
		Action: func(writer io.Writer, this *Server, arg string) (PrintStatusRule, error) {
			if isArgQuestion(arg) {
				fmt.Fprintf(writer, "%+v\n", this.migrationContext.GetNiceRatio())
				return NoPrintStatusRule, nil
			}
			if niceRatio, err := strconv.ParseFloat(arg, 64); err != nil {
				return NoPrintStatusRule, err
			} else {
				this.migrationContext.SetNiceRatio(niceRatio)
				return ForcePrintStatusAndHintRule, nil
			}
		},
	},
	{
		Name:      "max-load",
		Help:      "Set a new set of max-load thresholds",
		ValueType: StringValue,
		ValueHelp: "load",
		Action: func(writer io.Writer, this *Server, arg string) (PrintStatusRule, error) {
			if isArgQuestion(arg) {
				maxLoad := this.migrationContext.GetMaxLoad()
				fmt.Fprintf(writer, "%s\n", maxLoad.String())
				return NoPrintStatusRule, nil
			}
			if err := this.migrationContext.ReadMaxLoad(arg); err != nil {
				return NoPrintStatusRule, err
			}
			return ForcePrintStatusAndHintRule, nil
		},
	},
	{
		Name:      "critical-load",
		Help:      "Set a new set of max-load thresholds",
		ValueType: StringValue,
		ValueHelp: "load",
		Action: func(writer io.Writer, this *Server, arg string) (PrintStatusRule, error) {
			if isArgQuestion(arg) {
				criticalLoad := this.migrationContext.GetCriticalLoad()
				fmt.Fprintf(writer, "%s\n", criticalLoad.String())
				return NoPrintStatusRule, nil
			}
			if err := this.migrationContext.ReadCriticalLoad(arg); err != nil {
				return NoPrintStatusRule, err
			}
			return ForcePrintStatusAndHintRule, nil
		},
	},
	{
		Name:      "throttle-query",
		Help:      "Set a new throttle-query without quotes",
		ValueType: StringValue,
		ValueHelp: "query",
		Action: func(writer io.Writer, this *Server, arg string) (PrintStatusRule, error) {
			if isArgQuestion(arg) {
				fmt.Fprintf(writer, "%+v\n", this.migrationContext.GetThrottleQuery())
				return NoPrintStatusRule, nil
			}
			this.migrationContext.SetThrottleQuery(arg)
			fmt.Fprintln(writer, throttleHint)
			return ForcePrintStatusAndHintRule, nil
		},
	},
	{
		Name:      "throttle-http",
		Help:      "Set a new throttle URL",
		ValueType: StringValue,
		ValueHelp: "url",
		Action: func(writer io.Writer, this *Server, arg string) (PrintStatusRule, error) {
			if isArgQuestion(arg) {
				fmt.Fprintf(writer, "%+v\n", this.migrationContext.GetThrottleHTTP())
				return NoPrintStatusRule, nil
			}
			this.migrationContext.SetThrottleHTTP(arg)
			fmt.Fprintln(writer, throttleHint)
			return ForcePrintStatusAndHintRule, nil
		},
	},
	{
		Name:      "throttle-control-replicas",
		Help:      "Set a new comma delimited list of throttle control replicas",
		ValueType: StringValue,
		ValueHelp: "replicas",
		Action: func(writer io.Writer, this *Server, arg string) (PrintStatusRule, error) {
			if isArgQuestion(arg) {
				fmt.Fprintf(writer, "%s\n", this.migrationContext.GetThrottleControlReplicaKeys().ToCommaDelimitedList())
				return NoPrintStatusRule, nil
			}
			if err := this.migrationContext.ReadThrottleControlReplicaKeys(arg); err != nil {
				return NoPrintStatusRule, err
			}
			fmt.Fprintf(writer, "%s\n", this.migrationContext.GetThrottleControlReplicaKeys().ToCommaDelimitedList())
			return ForcePrintStatusAndHintRule, nil
		},
	},
	{
		Name:    "throttle",
		Aliases: []string{"pause", "suspend"},
		Help:    "Force throttle",
		Action: func(writer io.Writer, this *Server, arg string) (PrintStatusRule, error) {
			if arg != "" && arg != this.migrationContext.OriginalTableName {
				// User explicitly provided table name. This is a courtesy protection mechanism
				err := fmt.Errorf("User commanded 'throttle' on %s, but migrated table is %s; ignoring request.", arg, this.migrationContext.OriginalTableName)
				return NoPrintStatusRule, err
			}
			atomic.StoreInt64(&this.migrationContext.ThrottleCommandedByUser, 1)
			fmt.Fprintln(writer, throttleHint)
			return ForcePrintStatusAndHintRule, nil
		},
	},
	{
		Name:    "no-throttle",
		Aliases: []string{"unthrottle", "resume", "continue"},
		Help:    "End forced throttling (other throttling may still apply)",
		Action: func(writer io.Writer, this *Server, arg string) (PrintStatusRule, error) {
			if arg != "" && arg != this.migrationContext.OriginalTableName {
				// User explicitly provided table name. This is a courtesy protection mechanism
				err := fmt.Errorf("User commanded 'no-throttle' on %s, but migrated table is %s; ignoring request.", arg, this.migrationContext.OriginalTableName)
				return NoPrintStatusRule, err
			}
			atomic.StoreInt64(&this.migrationContext.ThrottleCommandedByUser, 0)
			return ForcePrintStatusAndHintRule, nil
		},
	},
	{
		Name:    "unpostpone",
		Aliases: []string{"no-postpone", "cut-over"},
		Help:    "Bail out a cut-over postpone; proceed to cut-over",
		Action: func(writer io.Writer, this *Server, arg string) (PrintStatusRule, error) {
			if arg == "" && this.migrationContext.ForceNamedCutOverCommand {
				err := fmt.Errorf("User commanded 'unpostpone' without specifying table name, but --force-named-cut-over is set")
				return NoPrintStatusRule, err
			}
			if arg != "" && arg != this.migrationContext.OriginalTableName {
				// User explicitly provided table name. This is a courtesy protection mechanism
				err := fmt.Errorf("User commanded 'unpostpone' on %s, but migrated table is %s; ignoring request.", arg, this.migrationContext.OriginalTableName)
				return NoPrintStatusRule, err
			}
			if atomic.LoadInt64(&this.migrationContext.IsPostponingCutOver) > 0 {
				atomic.StoreInt64(&this.migrationContext.UserCommandedUnpostponeFlag, 1)
				fmt.Fprintf(writer, "Unpostponed\n")
				return ForcePrintStatusAndHintRule, nil
			}
			fmt.Fprintf(writer, "You may only invoke this when gh-ost is actively postponing migration. At this time it is not.\n")
			return NoPrintStatusRule, nil
		},
	},
	{
		Name: "panic",
		Help: "Panic and quit without cleanup",
		Action: func(writer io.Writer, this *Server, arg string) (PrintStatusRule, error) {
			if arg == "" && this.migrationContext.ForceNamedPanicCommand {
				err := fmt.Errorf("User commanded 'panic' without specifying table name, but --force-named-panic is set")
				return NoPrintStatusRule, err
			}
			if arg != "" && arg != this.migrationContext.OriginalTableName {
				// User explicitly provided table name. This is a courtesy protection mechanism
				err := fmt.Errorf("User commanded 'panic' on %s, but migrated table is %s; ignoring request.", arg, this.migrationContext.OriginalTableName)
				return NoPrintStatusRule, err
			}
			err := fmt.Errorf("User commanded 'panic'. The migration will be aborted without cleanup. Please drop the gh-ost tables before trying again.")
			this.migrationContext.PanicAbort <- err
			return NoPrintStatusRule, err
		},
	},
}

func GetServerCommandOrAlias(commandOrAlias string) (*ServerCommand, error) {
	for _, cmd := range ServerCommands {
		if cmd.Name == commandOrAlias {
			return &cmd, nil
		} else {
			for _, alias := range cmd.Aliases {
				if alias == commandOrAlias {
					return &cmd, nil
				}
			}
		}
	}
	return nil, fmt.Errorf("Unknown command: %s", commandOrAlias)
}

func handleServerHelp(writer io.Writer) (PrintStatusRule, error) {
	sort.SliceStable(ServerCommands, func(i, j int) bool {
		return ServerCommands[i].Name < ServerCommands[j].Name
	})

	tabWriter := tabwriter.NewWriter(writer, 0, 8, 1, ' ', 0)
	for _, cmd := range ServerCommands {
		cmdNames := []string{cmd.Name}
		if len(cmd.Aliases) > 0 {
			sort.Strings(cmd.Aliases)
			cmdNames = append(cmdNames, cmd.Aliases...)
		}
		if cmd.ValueHelp != "" {
			fmt.Fprintf(tabWriter, "%s=<%s>\t# %s (%s)\n", strings.Join(cmdNames, ","), cmd.ValueHelp, cmd.Help, cmd.ValueType)
		} else {
			fmt.Fprintf(tabWriter, "%s\t# %s\n", strings.Join(cmdNames, ","), cmd.Help)
		}
	}
	if err := tabWriter.Flush(); err != nil {
		return NoPrintStatusRule, err
	}
	_, err := fmt.Fprintln(writer, `- use '?' (question mark) as argument to get info rather than set. e.g. "max-load=?" will just print out current max-load.`)

	return NoPrintStatusRule, err
}

// applyServerCommand parses and executes commands by user
func (this *Server) applyServerCommand(command string, writer io.Writer) (printStatusRule PrintStatusRule, err error) {
	printStatusRule = NoPrintStatusRule

	tokens := strings.SplitN(command, "=", 2)
	command = strings.TrimSpace(tokens[0])
	arg := ""
	if len(tokens) > 1 {
		arg = strings.TrimSpace(tokens[1])
		if unquoted, err := strconv.Unquote(arg); err == nil {
			arg = unquoted
		}
	}

	if err := this.hooksExecutor.onInteractiveCommand(command); err != nil {
		return NoPrintStatusRule, err
	}

	switch command {
	case "help":
		return handleServerHelp(writer)
	default:
		cmd, err := GetServerCommandOrAlias(command)
		if err != nil {
			return NoPrintStatusRule, err
		}

		if cmd.Action != nil {
			return cmd.Action(writer, this, arg)
		}
	}

	return NoPrintStatusRule, nil
}
