/*
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */

package runtime

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/gorundebug/servicelib/api"
	"github.com/gorundebug/servicelib/runtime/config"
	"github.com/gorundebug/servicelib/runtime/environment/log"
)

//go:embed web/status.html
var statusHtml []byte

//go:embed web/vis.min.js
var visMinJS []byte

//go:embed web/vis.min.css
var visMinCSS []byte

var englishUpperCaser = cases.Upper(language.English)

// MDI SVG path data (from @mdi/js), matching the stream graph editor icons.
const (
	mdiDatabaseArrowRight       = "M4 7C4 4.79 7.58 3 12 3S20 4.79 20 7 16.42 11 12 11 4 9.21 4 7M19.72 13.05C19.9 12.71 20 12.36 20 12V9C20 11.21 16.42 13 12 13S4 11.21 4 9V12C4 14.21 7.58 16 12 16C12.65 16 13.28 15.96 13.88 15.89C14.93 14.16 16.83 13 19 13C19.24 13 19.5 13 19.72 13.05M13.1 17.96C12.74 18 12.37 18 12 18C7.58 18 4 16.21 4 14V17C4 19.21 7.58 21 12 21C12.46 21 12.9 21 13.33 20.94C13.12 20.33 13 19.68 13 19C13 18.64 13.04 18.3 13.1 17.96M23 19L20 16V18H16V20H20V22L23 19Z"
	mdiArrowLeftRight           = "M6.45,17.45L1,12L6.45,6.55L7.86,7.96L4.83,11H19.17L16.14,7.96L17.55,6.55L23,12L17.55,17.45L16.14,16.04L19.17,13H4.83L7.86,16.04L6.45,17.45Z"
	mdiFilter                   = "M14,12V19.88C14.04,20.18 13.94,20.5 13.71,20.71C13.32,21.1 12.69,21.1 12.3,20.71L10.29,18.7C10.06,18.47 9.96,18.16 10,17.87V12H9.97L4.21,4.62C3.87,4.19 3.95,3.56 4.38,3.22C4.57,3.08 4.78,3 5,3V3H19V3C19.22,3 19.43,3.08 19.62,3.22C20.05,3.56 20.13,4.19 19.79,4.62L14.03,12H14Z"
	mdiCallMerge                = "M17,20.41L18.41,19L15,15.59L13.59,17M7.5,8H11V13.59L5.59,19L7,20.41L13,14.41V8H16.5L12,3.5"
	mdiFunction                 = "M15.6,5.29C14.5,5.19 13.53,6 13.43,7.11L13.18,10H16V12H13L12.56,17.07C12.37,19.27 10.43,20.9 8.23,20.7C6.92,20.59 5.82,19.86 5.17,18.83L6.67,17.33C6.91,18.07 7.57,18.64 8.4,18.71C9.5,18.81 10.47,18 10.57,16.89L11,12H8V10H11.17L11.44,6.93C11.63,4.73 13.57,3.1 15.77,3.3C17.08,3.41 18.18,4.14 18.83,5.17L17.33,6.67C17.09,5.93 16.43,5.36 15.6,5.29Z"
	mdiTransitConnectionVariant = "M18,11H14.82C14.4,9.84 13.3,9 12,9C10.7,9 9.6,9.84 9.18,11H6C5.67,11 4,10.9 4,9V8C4,6.17 5.54,6 6,6H16.18C16.6,7.16 17.7,8 19,8A3,3 0 0,0 22,5A3,3 0 0,0 19,2C17.7,2 16.6,2.84 16.18,4H6C4.39,4 2,5.06 2,8V9C2,11.94 4.39,13 6,13H9.18C9.6,14.16 10.7,15 12,15C13.3,15 14.4,14.16 14.82,13H18C18.33,13 20,13.1 20,15V16C20,17.83 18.46,18 18,18H7.82C7.4,16.84 6.3,16 5,16A3,3 0 0,0 2,19A3,3 0 0,0 5,22C6.3,22 7.4,21.16 7.82,20H18C19.61,20 22,18.93 22,16V15C22,12.07 19.61,11 18,11M19,4A1,1 0 0,1 20,5A1,1 0 0,1 19,6A1,1 0 0,1 18,5A1,1 0 0,1 19,4M5,20A1,1 0 0,1 4,19A1,1 0 0,1 5,18A1,1 0 0,1 6,19A1,1 0 0,1 5,20Z"
	mdiKey                      = "M7 14C5.9 14 5 13.1 5 12S5.9 10 7 10 9 10.9 9 12 8.1 14 7 14M12.6 10C11.8 7.7 9.6 6 7 6C3.7 6 1 8.7 1 12S3.7 18 7 18C9.6 18 11.8 16.3 12.6 14H16V18H20V14H23V10H12.6Z"
	mdiMerge                    = "M8 17L12 13H15.2C15.6 14.2 16.7 15 18 15C19.7 15 21 13.7 21 12S19.7 9 18 9C16.7 9 15.6 9.8 15.2 11H12L8 7V3H3V8H6L10.2 12L6 16H3V21H8V17Z"
	mdiCallSplit                = "M14,4L16.29,6.29L13.41,9.17L14.83,10.59L17.71,7.71L20,10V4M10,4H4V10L6.29,7.71L11,12.41V20H13V11.59L7.71,6.29"
	mdiSourceFork               = "M6,2A3,3 0 0,1 9,5C9,6.28 8.19,7.38 7.06,7.81C7.15,8.27 7.39,8.83 8,9.63C9,10.92 11,12.83 12,14.17C13,12.83 15,10.92 16,9.63C16.61,8.83 16.85,8.27 16.94,7.81C15.81,7.38 15,6.28 15,5A3,3 0 0,1 18,2A3,3 0 0,1 21,5C21,6.32 20.14,7.45 18.95,7.85C18.87,8.37 18.64,9 18,9.83C17,11.17 15,13.08 14,14.38C13.39,15.17 13.15,15.73 13.06,16.19C14.19,16.62 15,17.72 15,19A3,3 0 0,1 12,22A3,3 0 0,1 9,19C9,17.72 9.81,16.62 10.94,16.19C10.85,15.73 10.61,15.17 10,14.38C9,13.08 7,11.17 6,9.83C5.36,9 5.13,8.37 5.05,7.85C3.86,7.45 3,6.32 3,5A3,3 0 0,1 6,2M6,4A1,1 0 0,0 5,5A1,1 0 0,0 6,6A1,1 0 0,0 7,5A1,1 0 0,0 6,4M18,4A1,1 0 0,0 17,5A1,1 0 0,0 18,6A1,1 0 0,0 19,5A1,1 0 0,0 18,4M12,18A1,1 0 0,0 11,19A1,1 0 0,0 12,20A1,1 0 0,0 13,19A1,1 0 0,0 12,18Z"
	mdiDatabaseArrowLeft        = "M4 7C4 4.79 7.58 3 12 3S20 4.79 20 7 16.42 11 12 11 4 9.21 4 7M19.72 13.05C19.9 12.71 20 12.36 20 12V9C20 11.21 16.42 13 12 13S4 11.21 4 9V12C4 14.21 7.58 16 12 16C12.65 16 13.28 15.96 13.88 15.89C14.93 14.16 16.83 13 19 13C19.24 13 19.5 13 19.72 13.05M13.1 17.96C12.74 18 12.37 18 12 18C7.58 18 4 16.21 4 14V17C4 19.21 7.58 21 12 21C12.46 21 12.9 21 13.33 20.94C13.12 20.33 13 19.68 13 19C13 18.64 13.04 18.3 13.1 17.96M18 18V16L15 19L18 22V20H22V18H18Z"
	mdiSync                     = "M12,18A6,6 0 0,1 6,12C6,11 6.25,10.03 6.7,9.2L5.24,7.74C4.46,8.97 4,10.43 4,12A8,8 0 0,0 12,20V23L16,19L12,15M12,4V1L8,5L12,9V6A6,6 0 0,1 18,12C18,13 17.75,13.97 17.3,14.8L18.76,16.26C19.54,15.03 20,13.57 20,12A8,8 0 0,0 12,4Z"
	mdiAlertCircle              = "M13,13H11V7H13M13,17H11V15H13M12,2A10,10 0 0,0 2,12A10,10 0 0,0 12,22A10,10 0 0,0 22,12A10,10 0 0,0 12,2Z"
	mdiTimer                    = "M19.03 7.39L20.45 5.97C20 5.46 19.55 5 19.04 4.56L17.62 6C16.07 4.74 14.12 4 12 4C7.03 4 3 8.03 3 13S7.03 22 12 22C17 22 21 17.97 21 13C21 10.88 20.26 8.93 19.03 7.39M13 14H11V7H13V14M15 1H9V3H15V1Z"
	mdiSourceBranch             = "M13,14C9.64,14 8.54,15.35 8.18,16.24C9.25,16.7 10,17.76 10,19A3,3 0 0,1 7,22A3,3 0 0,1 4,19C4,17.69 4.83,16.58 6,16.17V7.83C4.83,7.42 4,6.31 4,5A3,3 0 0,1 7,2A3,3 0 0,1 10,5C10,6.31 9.17,7.42 8,7.83V13.12C8.88,12.47 10.16,12 12,12C14.67,12 15.56,10.66 15.85,9.77C14.77,9.32 14,8.25 14,7A3,3 0 0,1 17,4A3,3 0 0,1 20,7C20,8.34 19.12,9.5 17.91,9.86C17.65,11.29 16.68,14 13,14M7,18A1,1 0 0,0 6,19A1,1 0 0,0 7,20A1,1 0 0,0 8,19A1,1 0 0,0 7,18M7,4A1,1 0 0,0 6,5A1,1 0 0,0 7,6A1,1 0 0,0 8,5A1,1 0 0,0 7,4M17,6A1,1 0 0,0 16,7A1,1 0 0,0 17,8A1,1 0 0,0 18,7A1,1 0 0,0 17,6Z"
	mdiAPI                      = "M7 7H5A2 2 0 0 0 3 9V17H5V13H7V17H9V9A2 2 0 0 0 7 7M7 11H5V9H7M14 7H10V17H12V13H14A2 2 0 0 0 16 11V9A2 2 0 0 0 14 7M14 11H12V9H14M20 9V15H21V17H17V15H18V9H17V7H21V9Z"
	mdiCallMade                 = "M9,5V7H15.59L4,18.59L5.41,20L17,8.41V15H19V5"
)

var streamIconMap = map[api.TransformationType]string{
	api.TransformationTypeInput:           mdiDatabaseArrowRight,
	api.TransformationTypeMap:             mdiArrowLeftRight,
	api.TransformationTypeFilter:          mdiFilter,
	api.TransformationTypeJoin:            mdiCallMerge,
	api.TransformationTypeMultiJoin:       mdiCallMerge,
	api.TransformationTypeProcess:         mdiFunction,
	api.TransformationTypeFlatMap:         mdiTransitConnectionVariant,
	api.TransformationTypeFlatMapIterable: mdiTransitConnectionVariant,
	api.TransformationTypeKeyBy:           mdiKey,
	api.TransformationTypeMerge:           mdiMerge,
	api.TransformationTypeSplit:           mdiCallSplit,
	api.TransformationTypeCase:            mdiSourceFork,
	api.TransformationTypeSink:            mdiDatabaseArrowLeft,
	api.TransformationTypeCycleLink:       mdiSync,
	api.TransformationTypeError:           mdiAlertCircle,
	api.TransformationTypeDelay:           mdiTimer,
	api.TransformationTypeWhen:            mdiSourceBranch,
}

var svgReplacer = strings.NewReplacer(
	" ", "%20",
	"<", "%3C",
	">", "%3E",
	"#", "%23",
	`"`, "%22",
	"{", "%7B",
	"}", "%7D",
)

func svgDataURI(svg string) string {
	return "data:image/svg+xml;charset=utf-8," + svgReplacer.Replace(svg)
}

func makeNodeImageURI(iconPath, bgColor string, round bool) string {
	backgroundRadius := 10
	if round {
		backgroundRadius = 30
	}
	svg := fmt.Sprintf(
		`<svg xmlns="http://www.w3.org/2000/svg" width="60" height="60"><rect width="60" height="60" rx="%d" fill="%s"/><svg x="10" y="10" width="40" height="40" viewBox="0 0 24 24"><path d="%s" fill="white"/></svg></svg>`,
		backgroundRadius, bgColor, iconPath,
	)
	return svgDataURI(svg)
}

func makeNodeImageSelectedURI(iconPath, bgColor string, round bool) string {
	backgroundRadius := 10
	borderRadius := 9
	if round {
		backgroundRadius = 30
		borderRadius = 28
	}
	svg := fmt.Sprintf(
		`<svg xmlns="http://www.w3.org/2000/svg" width="60" height="60"><rect width="60" height="60" rx="%d" fill="%s"/><svg x="10" y="10" width="40" height="40" viewBox="0 0 24 24"><path d="%s" fill="white"/></svg><rect x="2" y="2" width="56" height="56" rx="%d" fill="none" stroke="#00FF80" stroke-width="4"/></svg>`,
		backgroundRadius, bgColor, iconPath, borderRadius,
	)
	return svgDataURI(svg)
}

type nodeImage struct {
	Unselected string `json:"unselected"`
	Selected   string `json:"selected"`
}

type nodeColor struct {
	Border    string `json:"border"`
	Highlight struct {
		Border string `json:"border"`
	} `json:"highlight"`
}

type Node struct {
	ID      int       `json:"id"`
	Label   string    `json:"label"`
	Shape   string    `json:"shape"`
	Image   nodeImage `json:"image"`
	Size    int       `json:"size"`
	Color   nodeColor `json:"color"`
	Opacity float32   `json:"opacity"`
	X       float64   `json:"x"`
	Y       float64   `json:"y"`
}

type Edge struct {
	From   int    `json:"from"`
	To     int    `json:"to"`
	Arrows string `json:"arrows"`
	Length int    `json:"length"`
	Label  string `json:"label"`
	Color  struct {
		Opacity float32 `json:"opacity"`
		Color   string  `json:"color"`
	} `json:"color"`
}

const (
	defaultOpacity = float32(1.0)
	edgeColor      = "#0050FF"
	edgeErrorColor = "#FF3030"
	edgeLength     = 200
)

type NetworkData struct {
	Nodes []*Node `json:"nodes"`
	Edges []*Edge `json:"edges"`
}

func (app *ServiceApp) makeNode(runtimeStream RuntimeStream) *Node {
	stream := runtimeStream.Stream()
	streamConfig := stream.GetConfig()
	serviceConfig := app.ServiceConfig()
	background := serviceConfig.Color
	serviceName := serviceConfig.Name
	if streamConfig.GetIdService() != serviceConfig.ID {
		for _, service := range app.GetConfig().GetServices() {
			if service.ID == streamConfig.GetIdService() {
				serviceName = service.Name
				background = service.Color
				break
			}
		}
	}

	label := fmt.Sprintf("%s(%s)\n[%s]", stream.GetName(),
		englishUpperCaser.String(stream.GetTransformationName()), serviceName)

	iconPath := statusIconPath(app.RuntimeConfig(), streamConfig)
	round := statusIconIsAPI(app.RuntimeConfig(), streamConfig)

	n := &Node{
		ID:    stream.GetID(),
		Label: label,
		Shape: "image",
		Image: nodeImage{
			Unselected: makeNodeImageURI(iconPath, background, round),
			Selected:   makeNodeImageSelectedURI(iconPath, background, round),
		},
		Size:    30,
		Opacity: defaultOpacity,
		X:       streamConfig.GetXPos(),
		Y:       streamConfig.GetYPos(),
	}
	n.Color.Border = "transparent"
	n.Color.Highlight.Border = "transparent"
	return n
}

func statusIconPath(runtimeConfig *config.RuntimeConfig, streamConfig config.StreamConfig) string {
	iconPath := mdiFunction
	if icon, ok := streamIconMap[streamConfig.GetType()]; ok {
		iconPath = icon
	}
	if statusIconIsAPI(runtimeConfig, streamConfig) {
		if streamConfig.GetType() == api.TransformationTypeSink {
			iconPath = mdiCallMade
		} else {
			iconPath = mdiAPI
		}
	}
	return iconPath
}

func statusIconIsAPI(runtimeConfig *config.RuntimeConfig, streamConfig config.StreamConfig) bool {
	endpointID := endpointIDForStatusIcon(streamConfig)
	if endpointID == 0 || runtimeConfig == nil {
		return false
	}
	endpoint := runtimeConfig.GetEndpointConfigByID(endpointID)
	if endpoint == nil {
		return false
	}
	connector := runtimeConfig.GetDataConnectorByID(endpoint.GetIdDataConnector())
	return connector != nil &&
		(connector.GetType() == api.DataConnectorTypeHTTP ||
			connector.GetType() == api.DataConnectorTypeGRPC)
}

func endpointIDForStatusIcon(stream config.StreamConfig) int {
	switch cfg := stream.(type) {
	case *config.InputStreamConfig:
		return cfg.IdEndpoint
	case *config.SinkStreamConfig:
		return cfg.IdEndpoint
	default:
		return 0
	}
}

func (app *ServiceApp) makeEdge(from Stream, typeName string, consumer Stream, color string) *Edge {
	label, _ := strings.CutPrefix(typeName, "*")
	label, _ = strings.CutPrefix(label, "types.")
	if stat, ok := app.consumeStatistics[config.LinkID{From: from.GetID(), To: consumer.GetID()}]; ok {
		label += fmt.Sprintf("\ncalls: %d", stat.Count())
	}
	cfg := consumer.GetConfig()
	if cfg.GetType() == api.TransformationTypeJoin ||
		cfg.GetType() == api.TransformationTypeMultiJoin {
		if cfg.GetIdSource() == from.GetID() {
			label += " (L)"
		} else {
			label += " (R)"
		}
	}
	return &Edge{
		From:   from.GetID(),
		To:     consumer.GetID(),
		Arrows: "to",
		Length: edgeLength,
		Label:  label,
		Color: struct {
			Opacity float32 `json:"opacity"`
			Color   string  `json:"color"`
		}{Opacity: defaultOpacity, Color: color},
	}
}

func (app *ServiceApp) makeEdgeFromTo(fromID int, statsFromID int, typeName string, toID int, color string) *Edge {
	label, _ := strings.CutPrefix(typeName, "*")
	label, _ = strings.CutPrefix(label, "types.")
	if stat, ok := app.consumeStatistics[config.LinkID{From: statsFromID, To: toID}]; ok {
		label += fmt.Sprintf("\ncalls: %d", stat.Count())
	}
	return &Edge{
		From:   fromID,
		To:     toID,
		Arrows: "to",
		Length: edgeLength,
		Label:  label,
		Color: struct {
			Opacity float32 `json:"opacity"`
			Color   string  `json:"color"`
		}{Opacity: defaultOpacity, Color: color},
	}
}

func (app *ServiceApp) makeEdges(runtimeStream RuntimeStream) []*Edge {
	edges := make([]*Edge, 0)
	stream := runtimeStream.Stream()

	for _, consumer := range runtimeStream.GetConsumers() {
		edges = append(edges, app.makeEdge(stream, stream.GetTypeName(), consumer, edgeColor))
	}

	if ec := runtimeStream.GetErrorConsumer(); ec != nil && len(ec.GetConsumers()) > 0 {
		virtualID := ec.Stream().GetID()
		edges = append(edges, app.makeEdgeFromTo(stream.GetID(), stream.GetID(), stream.GetTypeName(), virtualID, edgeErrorColor))
		errTypeName := ec.Stream().GetTypeName()
		for _, consumer := range ec.GetConsumers() {
			edges = append(edges, app.makeEdgeFromTo(virtualID, virtualID, errTypeName, consumer.GetID(), edgeColor))
		}
	}

	return edges
}

func (app *ServiceApp) makeErrorNode(producerStream RuntimeStream) *Node {
	stream := producerStream.Stream()
	streamConfig := stream.GetConfig()
	serviceConfig := app.ServiceConfig()
	background := serviceConfig.Color
	serviceName := serviceConfig.Name
	if streamConfig.GetIdService() != serviceConfig.ID {
		for _, service := range app.GetConfig().GetServices() {
			if service.ID == streamConfig.GetIdService() {
				serviceName = service.Name
				background = service.Color
				break
			}
		}
	}
	label := fmt.Sprintf("%s Error(%s)\n[%s]", stream.GetName(), "ERROR", serviceName)
	n := &Node{
		ID:    producerStream.GetErrorConsumer().Stream().GetID(),
		Label: label,
		Shape: "image",
		Image: nodeImage{
			Unselected: makeNodeImageURI(mdiAlertCircle, background, false),
			Selected:   makeNodeImageSelectedURI(mdiAlertCircle, background, false),
		},
		Size:    30,
		Opacity: defaultOpacity,
	}
	n.Color.Border = "transparent"
	n.Color.Highlight.Border = "transparent"
	return n
}

func (app *ServiceApp) statusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(statusHtml); err != nil {
		app.environment.Log().Warn(r.Context(), "statusHandler write error", log.Err(err))
	}
}

func (app *ServiceApp) visJSHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/javascript")
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(visMinJS); err != nil {
		app.environment.Log().Warn(r.Context(), "visJSHandler write error", log.Err(err))
	}
}

func (app *ServiceApp) visCSSHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/css")
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(visMinCSS); err != nil {
		app.environment.Log().Warn(r.Context(), "visCSSHandler write error", log.Err(err))
	}
}

func (app *ServiceApp) dataHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	networkData := NetworkData{
		Nodes: make([]*Node, 0, len(app.streams)),
		Edges: make([]*Edge, 0, len(app.streams)*2),
	}
	for _, stream := range app.streams {
		networkData.Nodes = append(networkData.Nodes, app.makeNode(stream))
		networkData.Edges = append(networkData.Edges, app.makeEdges(stream)...)
	}
	seen := make(map[int]bool)
	for _, stream := range app.streams {
		if ec := stream.GetErrorConsumer(); ec != nil && len(ec.GetConsumers()) > 0 {
			vid := ec.Stream().GetID()
			if !seen[vid] {
				seen[vid] = true
				networkData.Nodes = append(networkData.Nodes, app.makeErrorNode(stream))
			}
		}
	}
	jsonData, err := json.Marshal(networkData)
	if err != nil {
		http.Error(w, "Error serializing data to JSON", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if _, err := w.Write(jsonData); err != nil {
		app.environment.Log().Warn(r.Context(), "dataHandler write error", log.Err(err))
	}
}

func (app *ServiceApp) graphHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	streamApp := app.RuntimeToStreamApp()
	data, err := config.AppToYaml(streamApp)
	if err != nil {
		http.Error(w, "error serializing graph to YAML", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/yaml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(data); err != nil {
		app.environment.Log().Warn(r.Context(), "graphHandler write error", log.Err(err))
	}
}
