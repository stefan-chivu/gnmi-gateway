package statsd

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cactus/go-statsd-client/v5/statsd"
	"github.com/openconfig/gnmi-gateway/gateway/configuration"
	"github.com/openconfig/gnmi-gateway/gateway/connections"
	"github.com/openconfig/gnmi-gateway/gateway/exporters"
	"github.com/openconfig/gnmi-gateway/gateway/utils"
	"github.com/openconfig/gnmi/ctree"
	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
)

const Name = "statsd"
const MetricType = "metric"
const LogType = "log"
const LoggedMetricType = "loggedMetric"

var _ exporters.Exporter = new(StatsdExporter)

type Metric struct {
	Account     string            `json:",omitempty"`
	Measurement string            `json:"Metric"`
	Namespace   string            `json:"Namespace"`
	Dims        map[string]string `json:"Dims"`
	Value       int64             `json:"-"`
}

func init() {
	exporters.Register(Name, NewStatsdExporter)
}

func NewStatsdExporter(config *configuration.GatewayConfig) exporters.Exporter {
	return &StatsdExporter{
		config: config,
	}
}

type StatsdExporter struct {
	config  *configuration.GatewayConfig
	connMgr *connections.ConnectionManager
	client  statsd.Statter
}

func (e *StatsdExporter) Name() string {
	return Name
}

func (e *StatsdExporter) Export(leaf *ctree.Leaf) {
	notification := leaf.Value().(*gnmipb.Notification)

	for _, update := range notification.Update {
		notificationPath := utils.GetTrimmedPath(notification.Prefix, update.Path)

		notificationType := e.config.GetPathMetadata(notificationPath)["type"]
		e.config.Log.Debug().Msgf("Notification type: [ %s ]", notificationType)
		if notificationType != "" && notificationType != MetricType && notificationType != LoggedMetricType {
			continue
		}

		value, valid := utils.GetNumberValues(update.Val)
		if !valid {
			continue
		}

		metric := Metric{}

		metric.Value = int64(value)

		metric.Namespace = e.config.GetPathMetadata(notificationPath)["metricNamespace"]
		if metric.Namespace == "" {
			metric.Namespace = "Default"
		}

		elems, keys, err := extractPrefixAndPathKeys(notification.GetPrefix(), update.GetPath())
		if err != nil {
			e.config.Log.Info().Msg(fmt.Sprintf("Failed to extract path or keys: %s", err))
		}

		measurementPath := strings.Join(elems, "/")

		p := notification.GetPrefix()
		target := p.GetTarget()
		origin := p.GetOrigin()

		keys["path"] = measurementPath
		if origin != "" {
			keys["origin"] = origin
		}

		if target != "" {
			keys["target"] = target
		}

		metric.Measurement = pathToMetricName(measurementPath)

		metric.Dims = keys

		if e.connMgr != nil {
			targetConfig, found := (*e.connMgr).GetTargetConfig(metric.Dims["target"])

			if found {
				for _, fieldName := range e.config.ExporterMetadataAllowlist {
					fieldVal, exists := targetConfig.Meta[fieldName]
					if exists {
						metric.Dims[fieldName] = fieldVal
					}
				}
			} else {
				e.config.Log.Error().Msg("Target config not found for target: " + metric.Dims["target"])
				return
			}
		}

		if metric.Measurement == "" {
			e.config.Log.Info().Msg("Point measurement is empty. Returning.")
			return
		}

		// ns since epoch
		metric.Dims["timestamp"] = strconv.FormatInt(notification.Timestamp, 10)

		metric.Account = e.config.Exporters.GenevaMdmAccount

		if resourceId := e.config.Exporters.ExtensionArmId; resourceId != "" {
			metric.Dims["microsoft.resourceid"] = resourceId
		}

		metricJSON, err := json.Marshal(metric)

		if err != nil {
			e.config.Log.Error().Msg("Failed to marshal metric into JSON")
			return
		}

		e.config.Log.Debug().Msgf("%s:%d|g", string(metricJSON), metric.Value)
		if err := e.client.Gauge(string(metricJSON), metric.Value, 1); err != nil {
			e.config.Log.Error().Msg(err.Error())
		}
	}
}

func (e *StatsdExporter) Start(connMgr *connections.ConnectionManager) error {

	e.config.Log.Info().Msg("Starting Statsd exporter.")

	e.connMgr = connMgr

	var err error

	clientConfig := statsd.ClientConfig{
		Address:       e.config.Exporters.StatsdHost,
		UseBuffered:   true,
		FlushInterval: time.Duration(300) * time.Millisecond,
		FlushBytes:    0,
	}

	e.client, err = statsd.NewClientWithConfig(&clientConfig)

	if e.config.Exporters.GenevaMdmAccount == "" {
		e.config.Log.Warn().Msg("geneva MDM account is not set in exporter; metrics will be discarded by the MDM agent")
	}
	if e.config.Exporters.ExtensionArmId == "" {
		e.config.Log.Warn().Msg("extension ARM ID is not set in Geneva exporter; metrics will be discarded by the MDM agent")
	}

	return err
}

func pathToMetricName(metricPath string) string {
	return strings.ReplaceAll(strings.ReplaceAll(metricPath, "/", "_"), "-", "_")
}

func extractPrefixAndPathKeys(prefix *gnmipb.Path, metricPath *gnmipb.Path) ([]string, map[string]string, error) {
	elems := make([]string, 0)
	keys := make(map[string]string)
	for _, metricPath := range []*gnmipb.Path{prefix, metricPath} {
		for _, e := range metricPath.Elem {
			name := e.Name
			elems = append(elems, name)
			for k, v := range e.GetKey() {
				keys[e.Name+"_"+k] = v
			}
		}
	}

	if len(elems) == 0 {
		return elems, keys, errors.New("path contains no elems")
	}

	return elems, keys, nil
}
