package ekanite

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"
	"github.com/labstack/echo"
)

var (
	timeFormats = []string{
		"2006-01-02T15:04:05.000Z07:00",
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
		"2006-01-02T15:04:05.999999999 07:00",
		"2006-01-02T15:04:05 07:00"}
)

func parseTime(s string) time.Time {
	for _, layout := range timeFormats {
		v, err := time.ParseInLocation(layout, s, time.Local)
		if err == nil {
			return v.Local()
		}
	}
	return time.Time{}
}

func isConsumeJSON(r *http.Request) bool {
	accept := r.Header.Get("Accept")
	contentType := r.Header.Get(echo.HeaderContentType)
	return strings.Contains(contentType, "application/json") &&
		strings.Contains(accept, "application/json")
}

func encodeJSON(w io.Writer, i interface{}) error {
	if headered, ok := w.(http.ResponseWriter); ok {
		headered.Header().Set("Cache-Control", "no-cache")
		headered.Header().Set("Content-type", "application/json")
	}

	e := json.NewEncoder(w)
	return e.Encode(i)
}

// HTTPServer serves query client connections.
type HTTPServer struct {
	iface    string
	Searcher Searcher

	addr     net.Addr
	template *template.Template

	Logger *log.Logger
}

// NewHTTPServer returns a new Server instance.
func NewHTTPServer(iface string, searcher Searcher) *HTTPServer {
	return &HTTPServer{
		iface:    iface,
		Searcher: searcher,
		Logger:   log.New(os.Stderr, "[httpserver] ", log.LstdFlags),
	}
}

// Start instructs the Server to bind to the interface and accept connections.
func (s *HTTPServer) Start() error {
	ln, err := net.Listen("tcp", s.iface)
	if err != nil {
		return err
	}

	s.addr = ln.Addr()

	s.template, err = template.New("ServerTemplate").Parse(templateSource)
	if err != nil {
		ln.Close()
		return err
	}

	go http.Serve(ln, s)
	return nil
}

// Addr returns the address to which the Server is bound.
func (s *HTTPServer) Addr() net.Addr {
	return s.addr
}

// ServeHTTP implements a http.Handler, serving the query interface for Ekanite
func (s *HTTPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	dontCache(w, r)

	if isConsumeJSON(r) {
		if strings.HasSuffix(r.URL.Path, "/_summary") {
			s.Summary(w, r)
		} else {
			s.Get(w, r)
		}
		return
	}
	s.QueryHTML(w, r)
}

func (s *HTTPServer) Summary(w http.ResponseWriter, req *http.Request) {
	s.Search(w, req, func(req *bleve.SearchRequest, resp *SearchResult) error {
		return encodeJSON(w, resp.SearchResult)
	})
}

func (s *HTTPServer) Get(w http.ResponseWriter, req *http.Request) {
	s.Search(w, req, func(req *bleve.SearchRequest, resp *SearchResult) error {
		var documents = make([]interface{}, 0, resp.DocumentHits.Len())
		for _, doc := range resp.DocumentHits {
			bs, err := doc.Index.GetInternal([]byte(doc.Doc.ID))
			if err != nil {
				return err
			}
			jsRaw := json.RawMessage(bs)
			documents = append(documents, &jsRaw)
		}
		return encodeJSON(w, resp.SearchResult)
	})
}

func (s *HTTPServer) Search(w http.ResponseWriter, req *http.Request, cb func(req *bleve.SearchRequest, resp *SearchResult) error) {
	queryParams := req.URL.Query()

	startAt := queryParams.Get("start_at")
	if startAt == "" {
		http.Error(w, "start_at is required.", http.StatusBadRequest)
		return
	}

	var start = parseTime(startAt)
	if start.IsZero() {
		http.Error(w, "start_at("+startAt+") is invalid.", http.StatusBadRequest)
		return
	}

	var end time.Time
	if endAt := queryParams.Get("end_at"); endAt != "" {
		end = parseTime(endAt)
		if end.IsZero() {
			http.Error(w, "end_at("+endAt+") is invalid.", http.StatusBadRequest)
			return
		}
	} else {
		end = time.Now()
	}

	var searchRequest *bleve.SearchRequest
	if req.Method == "GET" {
		q := queryParams.Get("q")
		if q == "" {
			http.Error(w, "q is required.", http.StatusBadRequest)
			return
		}

		limit := maxSearchHitSize
		offset := 0

		if limitStr := queryParams.Get("limit"); limitStr != "" {
			i64, err := strconv.ParseInt(limitStr, 10, 0)
			if err != nil {
				http.Error(w, "limit("+limitStr+") is invalid.", http.StatusBadRequest)
				return
			}
			limit = int(i64)
			if limit <= 0 {
				limit = maxSearchHitSize
			}
		}

		if offsetStr := queryParams.Get("offset"); offsetStr != "" {
			i64, err := strconv.ParseInt(offsetStr, 10, 0)
			if err != nil {
				http.Error(w, "offset("+offsetStr+") is invalid.", http.StatusBadRequest)
				return
			}
			offset = int(i64)
			if offset < 0 {
				offset = 0
			}
		}

		query := bleve.NewQueryStringQuery(q)
		searchRequest := bleve.NewSearchRequest(query)
		searchRequest.Size = limit
		searchRequest.From = offset
	} else {
		requestBody, err := ioutil.ReadAll(req.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("error reading request body: %v", err), http.StatusBadRequest)
			return
		}
		//logger.Printf("request body: %s", requestBody)

		searchRequest = new(bleve.SearchRequest)
		err = json.Unmarshal(requestBody, searchRequest)
		if err != nil {
			http.Error(w, fmt.Sprintf("error parsing query: %v", err), http.StatusBadRequest)
			return
		}
	}

	//logger.Printf("parsed request %#v", searchRequest)

	// validate the query
	if srqv, ok := searchRequest.Query.(query.ValidatableQuery); ok {
		err := srqv.Validate()
		if err != nil {
			http.Error(w, fmt.Sprintf("error validating query: %v", err), http.StatusBadRequest)
			return
		}
	}

	// execute the query
	err := s.Searcher.Query(start, end, searchRequest, cb)
	if err != nil {
		http.Error(w, fmt.Sprintf("error executing query: %v", err), http.StatusInternalServerError)
		return
	}
}

func (s *HTTPServer) QueryHTML(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" || r.Method == "HEAD" {
		// HEAD is conveniently supported by net/http without further action
		err := serveIndex(s, w, r)

		if err != nil {
			s.Logger.Print("Error executing template: ", err)
			http.Error(w, "Error executing template: "+err.Error(), http.StatusInternalServerError)
			return
		}
	} else if r.Method == "POST" {
		err := r.ParseForm()
		if err != nil {
			s.Logger.Printf("Error parsing form '%s'", err)
			http.Error(w, "Error parsing form", http.StatusBadRequest)
			return
		}

		if len(r.FormValue("query")) == 0 {
			serveIndex(s, w, r)
			return
		}

		userQuery := r.FormValue("query")
		s.Logger.Printf("executing query '%s'", userQuery)

		start := time.Now()
		resultSet, err := SearchString(s.Logger, s.Searcher, userQuery)
		dur := time.Since(start)
		var resultSlice []string

		if err != nil {
			s.Logger.Printf("Error executing query: '%s'", err)
			http.Error(w, "Error executing query: "+err.Error(), http.StatusInternalServerError)
			return
		}

		for s := range resultSet {
			resultSlice = append(resultSlice, s)
		}

		data := struct {
			Title         string
			Headline      string
			ReturnResults bool
			LogMessages   []string
		}{
			"Ekanite query interface",
			fmt.Sprintf(`Ekanite - Listing %d results for "%s" (%s)`, len(resultSlice), userQuery, dur.String()),
			true,
			resultSlice,
		}

		if err := s.template.Execute(w, data); err != nil {
			s.Logger.Print("Error executing template: ", err)
		}
	} else {
		http.Error(w, "Unsupported method", http.StatusMethodNotAllowed)
	}
}

// serveIndex serves the plain index for the GET request and POST failovers
func serveIndex(s *HTTPServer, w http.ResponseWriter, r *http.Request) error {
	data := struct {
		Title         string
		Headline      string
		ReturnResults bool
		LogMessages   []string
	}{
		"Ekanite query interface",
		"Ekanite query interface",
		false,
		[]string{},
	}

	return s.template.Execute(w, data)
}

func SearchString(logger *log.Logger, searcher Searcher, q string) (<-chan string, error) {
	query := bleve.NewQueryStringQuery(q)
	searchRequest := bleve.NewSearchRequest(query)
	searchRequest.Size = maxSearchHitSize

	// validate the query
	err := query.Validate()
	if err != nil {
		return nil, err
	}

	// Buffer channel to control how many docs are sent back.
	c := make(chan string, 1)
	go func() {
		// execute the query
		err := searcher.Query(time.Time{}, time.Now(), searchRequest, func(req *bleve.SearchRequest, resp *SearchResult) error {
			for _, doc := range resp.DocumentHits {
				bs, err := doc.Index.GetInternal([]byte(doc.Doc.ID))
				if err != nil {
					return err
				}
				c <- string(bs)
			}
			return nil
		})
		if err != nil {
			logger.Println("error getting document:", err.Error())
		}
	}()

	return c, nil
}

// dontCache sets necessary headers to avoid client and intermediate caching of response
func dontCache(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Expires", time.Unix(0, 0).Format(time.RFC1123))
	w.Header().Set("Last-Modified", time.Now().Format(time.RFC1123))
	w.Header().Set("Cache-Control", "private, no-store, max-age=0, no-cache, must-revalidate, post-check=0, pre-check=0")
	return
}

const templateSource string = `
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8" />
<title>{{ $.Title }}</title>
<style type="text/css">
body, h2 {
	margin: 50px;
	font-family: sans-serif;
	font-size: 13px;
}
h2 {
	font-size: 15px;
}
.button {
	background: #3498db;
	background-image: linear-gradient(to bottom, #3498db, #2980b9);
	border-radius: 4px;
	font-family: sans-serif;
	color: #ffffff;
	font-size: 15px;
	padding: 10px 20px 10px 20px;
	margin-bottom: 20px;
	text-decoration: none;
}
hr {
	margin-bottom: 10px;
	margin-top: 10px;
}
.button:hover {
	background: #3cb0fd;
	background-image: linear-gradient(to bottom, #3cb0fd, #3498db);
	text-decoration: none;
}
textarea {
	margin: 20px 20px 20px 0;
}
</style>
</head>
<body>
	<h2>{{ $.Headline }}</h2>
	<div id="help">Query language reference: <a href="http://godoc.org/github.com/blevesearch/bleve#NewQueryStringQuery">bleve</a></div>
	<form action="/" method="POST">
    <textarea name="query" cols="100" rows="2"></textarea>
    <br>
    <input name="submit" type="submit" class="button" value="Query">
	</form>

{{ if $.ReturnResults }}
	<hr>
	<ul>
	{{range $message := $.LogMessages }}
	<li>{{ $message }}</li>
	{{ end }}
	</ul>
{{ end }}
</body>
</html>
`
