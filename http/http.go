package http

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"
	"github.com/ekanite/ekanite"
	"github.com/labstack/echo"
	"github.com/runner-mei/borm"
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

func encodeJSON(w http.ResponseWriter, i interface{}) error {
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Content-type", "application/json")

	e := json.NewEncoder(w)
	return e.Encode(i)
}

// HTTPServer serves query client connections.
type HTTPServer struct {
	addr     string
	Searcher ekanite.Searcher
	DB       *borm.Bucket

	//engine *echo.Echo
	Logger *log.Logger
}

// NewHTTPServer returns a new Server instance.
func NewHTTPServer(addr string, searcher ekanite.Searcher, db *borm.Bucket) *HTTPServer {
	return &HTTPServer{
		addr:     addr,
		Searcher: searcher,
		DB:       db,
		Logger:   log.New(os.Stderr, "[httpserver] ", log.LstdFlags),
	}
}

// Start instructs the Server to bind to the interface and accept connections.
func (s *HTTPServer) Start() error {
	return http.ListenAndServe(s.addr, s)
}

// ServeHTTP implements a http.Handler, serving the query interface for Ekanite
func (s *HTTPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if o := recover(); o != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintln(w, o)
			w.Write(debug.Stack())
		}
	}()
	if strings.HasPrefix(r.URL.Path, "/debug/") {
		http.DefaultServeMux.ServeHTTP(w, r)
		return
	}
	if r.URL.Path == "/fields" {
		s.Fields(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/query/") {
		filterName := strings.TrimPrefix(r.URL.Path, "/query/")
		if filterName != "" {
			if strings.HasSuffix(filterName, "/count") {
				filterName = strings.TrimSuffix(filterName, "/count")
				s.SummaryByFilters(w, r, filterName)
			} else {
				s.SearchByFilters(w, r, filterName)
			}
			return
		}

		http.DefaultServeMux.ServeHTTP(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/filters") {
		pa := strings.Trim(strings.TrimPrefix(r.URL.Path, "/filters"), "/")
		switch r.Method {
		case "GET":
			if pa == "" {
				s.ListFilterIDs(w, r)
			} else {
				s.ReadFilter(w, r, pa)
			}
		case "POST":
			if pa != "" {
				w.WriteHeader(http.StatusMethodNotAllowed)
				w.Write([]byte("MethodNotAllowed"))
			} else {
				s.CreateFilter(w, r)
			}
		case "DELETE":
			if pa == "" {
				w.WriteHeader(http.StatusMethodNotAllowed)
				w.Write([]byte("MethodNotAllowed"))
			} else {
				s.DeleteFilter(w, r, pa)
			}
		case "PUT":
			if pa == "" {
				w.WriteHeader(http.StatusMethodNotAllowed)
				w.Write([]byte("MethodNotAllowed"))
			} else {
				s.UpdateFilter(w, r, pa)
			}
		default:
			http.DefaultServeMux.ServeHTTP(w, r)
		}
		return
	}

	if strings.HasPrefix(r.URL.Path, "/fields/") {
		field := strings.TrimPrefix(r.URL.Path, "/fields/")
		if field == "" {
			s.Fields(w, r)
		} else {
			s.FieldDict(w, r, field)
		}
		return
	}

	if r.URL.Path == "/summary" {
		s.Summary(w, r)
		return
	}

	if r.URL.Path == "/" {
		s.Get(w, r)
		return
	}
	http.DefaultServeMux.ServeHTTP(w, r)
}

func (s *HTTPServer) Summary(w http.ResponseWriter, req *http.Request) {
	s.Search(w, req, false, func(req *bleve.SearchRequest, resp *bleve.SearchResult) error {
		return encodeJSON(w, resp.Total)
	})
}

func (s *HTTPServer) Get(w http.ResponseWriter, req *http.Request) {
	s.Search(w, req, true, func(req *bleve.SearchRequest, resp *bleve.SearchResult) error {
		var documents = make([]interface{}, 0, resp.Hits.Len())
		for _, doc := range resp.Hits {
			documents = append(documents, doc.Fields)
		}
		return encodeJSON(w, documents)
	})
}

func (s *HTTPServer) FieldDict(w http.ResponseWriter, req *http.Request, field string) {
	s.timeRange(w, req, func(w http.ResponseWriter, req *http.Request, start, end time.Time) {
		entries, err := s.Searcher.FieldDict(start, end, field)
		if err != nil {
			http.Error(w, fmt.Sprintf("error get field dicts: %v", err), http.StatusInternalServerError)
			return
		}
		if err := encodeJSON(w, entries); err != nil {
			http.Error(w, fmt.Sprintf("error get field dicts: %v", err), http.StatusInternalServerError)
		}
	})
}

func (s *HTTPServer) Fields(w http.ResponseWriter, req *http.Request) {
	s.timeRange(w, req, func(w http.ResponseWriter, req *http.Request, start, end time.Time) {
		fields, err := s.Searcher.Fields(start, end)
		if err != nil {
			http.Error(w, fmt.Sprintf("error get fields: %v", err), http.StatusInternalServerError)
			return
		}
		if err := encodeJSON(w, fields); err != nil {
			http.Error(w, fmt.Sprintf("error get fields: %v", err), http.StatusInternalServerError)
		}
	})
}

func (s *HTTPServer) timeRange(w http.ResponseWriter, req *http.Request,
	cb func(w http.ResponseWriter, req *http.Request, start, end time.Time)) {
	queryParams := req.URL.Query()

	var start, end time.Time

	startAt := queryParams.Get("start_at")
	if startAt != "" {
		start = parseTime(startAt)
		if start.IsZero() {
			http.Error(w, "start_at("+startAt+") is invalid.", http.StatusBadRequest)
			return
		}
	}

	if endAt := queryParams.Get("end_at"); endAt != "" {
		end = parseTime(endAt)
		if end.IsZero() {
			http.Error(w, "end_at("+endAt+") is invalid.", http.StatusBadRequest)
			return
		}
	}

	cb(w, req, start, end)
}

func (s *HTTPServer) Search(w http.ResponseWriter, req *http.Request, allFields bool, cb func(req *bleve.SearchRequest, resp *bleve.SearchResult) error) {
	var searchRequest *bleve.SearchRequest
	if req.Method == "GET" {
		queryParams := req.URL.Query()
		q := queryParams.Get("q")
		if q == "" {
			http.Error(w, "q is required.", http.StatusBadRequest)
			return
		}

		query := bleve.NewQueryStringQuery(q)
		searchRequest = bleve.NewSearchRequest(query)
	} else {
		requestBody, err := ioutil.ReadAll(req.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("error reading request body: %v", err), http.StatusBadRequest)
			return
		}

		searchRequest = new(bleve.SearchRequest)
		err = json.Unmarshal(requestBody, searchRequest)
		if err != nil {
			http.Error(w, fmt.Sprintf("error parsing query: %v", err), http.StatusBadRequest)
			return
		}
	}

	if allFields {
		searchRequest.Fields = []string{"*"}
	}

	s.SearchIn(w, req, searchRequest, cb)
}

func (s *HTTPServer) SearchIn(w http.ResponseWriter, req *http.Request, searchRequest *bleve.SearchRequest, cb func(req *bleve.SearchRequest, resp *bleve.SearchResult) error) {
	queryParams := req.URL.Query()

	var start, end time.Time

	startAt := queryParams.Get("start_at")
	if startAt != "" {
		start = parseTime(startAt)
		if start.IsZero() {
			http.Error(w, "start_at("+startAt+") is invalid.", http.StatusBadRequest)
			return
		}
	}

	if endAt := queryParams.Get("end_at"); endAt != "" {
		end = parseTime(endAt)
		if end.IsZero() {
			http.Error(w, "end_at("+endAt+") is invalid.", http.StatusBadRequest)
			return
		}
	}

	if !start.IsZero() || !end.IsZero() {
		inclusive := true
		timeQuery := bleve.NewDateRangeInclusiveQuery(start, end, &inclusive, &inclusive)
		timeQuery.SetField("reception")

		if searchRequest.Query == nil {
			searchRequest.Query = timeQuery
		} else if conjunctionQuery, ok := searchRequest.Query.(*query.ConjunctionQuery); ok {
			conjunctionQuery.AddQuery(timeQuery)
		} else {
			searchRequest.Query = bleve.NewConjunctionQuery(searchRequest.Query, timeQuery)
		}
	} else if searchRequest.Query == nil {
		inclusive := true
		timeQuery := bleve.NewDateRangeInclusiveQuery(start, time.Now(), &inclusive, &inclusive)
		timeQuery.SetField("reception")

		searchRequest.Query = timeQuery
	}

	// var searchRequest *bleve.SearchRequest
	// query := bleve.NewConjunctionQuery(queries...)
	// searchRequest = bleve.NewSearchRequest(query)
	// searchRequest.SortBy([]string{"timestamp"})

	{
		if limitStr := queryParams.Get("limit"); limitStr != "" {
			i64, err := strconv.ParseInt(limitStr, 10, 0)
			if err != nil {
				http.Error(w, "limit("+limitStr+") is invalid.", http.StatusBadRequest)
				return
			}
			limit := int(i64)
			if limit <= 0 {
				limit = ekanite.MaxSearchHitSize
			}
			searchRequest.Size = limit
		}

		if offsetStr := queryParams.Get("offset"); offsetStr != "" {
			i64, err := strconv.ParseInt(offsetStr, 10, 0)
			if err != nil {
				http.Error(w, "offset("+offsetStr+") is invalid.", http.StatusBadRequest)
				return
			}
			offset := int(i64)
			if offset < 0 {
				offset = 0
			}
			searchRequest.From = offset
		}
	}

	if sortBy := queryParams.Get("sort_by"); sortBy != "" {
		searchRequest.SortBy([]string{sortBy})
	}

	// if allFields {
	// 	searchRequest.Fields = []string{"*"}
	// }
	bs, _ := json.Marshal(searchRequest)
	s.Logger.Printf("parsed request %s", string(bs))

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

/*
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
			"query",
			fmt.Sprintf(`Listing %d results for "%s" (%s)`, len(resultSlice), userQuery, dur.String()),
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

*/

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
