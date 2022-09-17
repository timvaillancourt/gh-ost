package client

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"strings"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/logic"
	"github.com/github/gh-ost/go/mysql"
)

type Client struct {
	network string
	address string
}

func New(network, address string) *Client {
	return &Client{network, address}
}

func buildClientCommand(command string, set interface{}) (string, error) {
	cmd, err := logic.GetServerCommandOrAlias(command)
	if err != nil {
		return command, err
	}

	if cmd.ValueHelp != "" {
		if set != nil {
			command = fmt.Sprintf("%s=%v", command, set) // set value
		} else {
			command = fmt.Sprintf("%s=?", command) // get value (?)
		}
	}

	return command, nil
}

func handleClientCommandResult(result []byte, out interface{}) (err error) {
	if out == nil || len(result) == 0 {
		return nil
	}

	errUnuspportedOut := errors.New("'out' must be a pointer to a float64, int64, string, base.LoadMap or mysql.InstanceKeyMap")
	val := reflect.ValueOf(out)
	if val.Kind() != reflect.Ptr {
		return errUnuspportedOut
	}

	resultStr := strings.TrimSpace(string(result))

	var setOut interface{}
	switch out.(type) {
	case *base.LoadMap:
		setOut, err = base.ParseLoadMap(resultStr)
	case *mysql.InstanceKeyMap:
		keys := mysql.NewInstanceKeyMap()
		err = keys.ReadCommaDelimitedList(resultStr)
		setOut = *keys
	case *float64:
		setOut, err = strconv.ParseFloat(resultStr, 64)
	case *int64:
		setOut, err = strconv.ParseInt(resultStr, 10, 64)
	case *string:
		setOut = resultStr
	default:
		return errUnuspportedOut
	}

	if err == nil {
		val.Elem().Set(reflect.ValueOf(setOut))
	}
	return err
}

func (c *Client) doCommand(command string, set, out interface{}) error {
	// open a connection for every command because the server closes it after every command
	conn, err := net.Dial(c.network, c.address)
	if err != nil {
		return err
	}
	defer conn.Close()

	cmd, err := buildClientCommand(command, set)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(conn, cmd)
	if err != nil {
		return err
	}

	result := make([]byte, 0)
	reader := bufio.NewReader(conn)
	for {
		read, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		result = append(result, read...)
	}

	return handleClientCommandResult(result, out)
}

// get commands

func (c *Client) GetApplier() (applier string, err error) {
	err = c.doCommand("applier", nil, &applier)
	return applier, err
}

func (c *Client) GetChunkSize() (size int64, err error) {
	err = c.doCommand("chunk-size", nil, &size)
	return size, err
}

func (c *Client) GetCoordinates() (coordinates string, err error) {
	err = c.doCommand("coordinates", nil, &coordinates)
	return coordinates, err
}

func (c *Client) GetCriticalLoad() (loadMap base.LoadMap, err error) {
	err = c.doCommand("critical-load", nil, &loadMap)
	return loadMap, err
}

func (c *Client) GetDMLBatchSize() (size int64, err error) {
	err = c.doCommand("dml-batch-size", nil, &size)
	return size, err
}

func (c *Client) GetHelp() (help string, err error) {
	err = c.doCommand("help", nil, &help)
	return help, err
}

func (c *Client) GetInspector() (inspector string, err error) {
	err = c.doCommand("inspector", nil, &inspector)
	return inspector, err
}
func (c *Client) GetMaxLagMillis() (maxLag int64, err error) {
	err = c.doCommand("max-lag-millis", nil, &maxLag)
	return maxLag, err
}

func (c *Client) GetMaxLoad() (loadMap base.LoadMap, err error) {
	err = c.doCommand("max-load", nil, &loadMap)
	return loadMap, err
}

func (c *Client) GetNiceRatio() (ratio float64, err error) {
	err = c.doCommand("nice-ratio", nil, &ratio)
	return ratio, err
}

func (c *Client) GetThrottleControlReplicas() (replicas mysql.InstanceKeyMap, err error) {
	err = c.doCommand("throttle-control-replicas", nil, &replicas)
	return replicas, err
}

func (c *Client) GetThrottleQuery() (query string, err error) {
	err = c.doCommand("throttle-query", nil, &query)
	return query, err
}

func (c *Client) GetVersion() (version string, err error) {
	err = c.doCommand("version", nil, &version)
	return version, err
}

// set commands

func (c *Client) SetDMLBatchSize(size int64) error {
	return c.doCommand("dml-batch-size", size, nil)
}

func (c *Client) SetPanic(table string) error {
	return c.doCommand("panic", table, nil)
}

func (c *Client) SetMaxLagMillis(lagMillis int64) error {
	return c.doCommand("max-lag-millis", lagMillis, nil)
}

func (c *Client) SetMaxLoad(load string) error {
	return c.doCommand("max-load", load, nil)
}

func (c *Client) SetNiceRatio(ratio float64) error {
	return c.doCommand("nice-ratio", ratio, nil)
}

func (c *Client) SetThrottleControlReplicas(replicas *mysql.InstanceKeyMap) error {
	return c.doCommand("throttle-control-replicas", replicas.ToCommaDelimitedList(), nil)
}

func (c *Client) SetThrottleQuery(query string) error {
	return c.doCommand("throttle-query", query, nil)
}

// bool commands

func (c *Client) Throttle() error {
	return c.doCommand("throttle", nil, nil)
}

func (c *Client) NoThrottle() error {
	return c.doCommand("no-throttle", nil, nil)
}

// UnThrottle is an alias for NoThrottle()
func (c *Client) UnThrottle() error {
	return c.NoThrottle()
}

func (c *Client) Unpostpone() error {
	return c.doCommand("unpostpone", nil, nil)
}

// CutOver is an alias for Unpostpone()
func (c *Client) CutOver() error {
	return c.Unpostpone()
}
