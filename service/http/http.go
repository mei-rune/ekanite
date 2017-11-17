package http

import (
	"bytes"
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

	"github.com/ekanite/ekanite/input"
	"github.com/ekanite/ekanite/service"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"
	"github.com/ekanite/ekanite"
	"github.com/labstack/echo"
)

func isConsumeJSON(r *http.Request) bool {
	accept := r.Header.Get("Accept")
	contentType := r.Header.Get(echo.HeaderContentType)
	return strings.Contains(contentType, "application/json") &&
		strings.Contains(accept, "application/json")
}

func renderJSON(w http.ResponseWriter, i interface{}) {
	if err := encodeJSON(w, i); err != nil {
		log.Println("[WARN]", err)
	}
}

func encodeJSON(w http.ResponseWriter, i interface{}) error {
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Content-type", "application/json")

	e := json.NewEncoder(w)
	return e.Encode(i)
}

func decodeJSON(req *http.Request, i interface{}) error {
	decoder := json.NewDecoder(req.Body)
	return decoder.Decode(i)
}

// Server serves query client connections.
type Server struct {
	addr      string
	urlPrefix string
	c         chan<- ekanite.Document
	Searcher  ekanite.Searcher
	metaStore *service.MetaStore

	NoRoute http.Handler
	//engine *echo.Echo
	Logger *log.Logger
}

// NewServer returns a new Server instance.
func NewServer(addr, urlPrefix string, c chan<- ekanite.Document,
	searcher ekanite.Searcher, metaStore *service.MetaStore) *Server {
	return &Server{
		addr:      addr,
		urlPrefix: urlPrefix,
		c:         c,
		Searcher:  searcher,
		metaStore: metaStore,
		Logger:    log.New(os.Stderr, "[httpserver] ", log.LstdFlags),
	}
}

// Start instructs the Server to bind to the interface and accept connections.
func (s *Server) Start() error {
	return http.ListenAndServe(s.addr, s)
}

// SplitURLPath 分隔 url path, 取出 url path 的第一部份
func SplitURLPath(pa string) (string, string) {
	if "" == pa {
		return "", ""
	}

	if '/' == pa[0] {
		pa = pa[1:]
	}

	idx := strings.IndexRune(pa, '/')
	if idx < 0 {
		return pa, ""
	}
	return pa[:idx], pa[idx:]
}

// ServeHTTP implements a http.Handler, serving the query interface for Ekanite
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if o := recover(); o != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintln(w, o)
			w.Write(debug.Stack())
		}
	}()

	if !strings.HasPrefix(r.URL.Path, s.urlPrefix) {
		if s.NoRoute == nil {
			http.DefaultServeMux.ServeHTTP(w, r)
		} else {
			s.NoRoute.ServeHTTP(w, r)
		}
		return
	}

	name, pa := SplitURLPath(strings.TrimPrefix(r.URL.Path, s.urlPrefix))
	switch name {
	case "debug":
		http.DefaultServeMux.ServeHTTP(w, r)
		return
	case "fields":
		if pa == "" || pa == "/" {
			s.Fields(w, r)
		} else {
			s.FieldDict(w, r, strings.Trim(pa, "/"))
		}
		return
	case "query":
		switch pa {
		case "", "/":
			if r.Method == "POST" {
				s.SearchByFiltersInBody(w, r)
				return
			}
		case "/count", "/count/":
			if r.Method == "POST" {
				s.SummaryByFiltersInBody(w, r)
				return
			}
		default:
			if strings.HasSuffix(pa, "/count") {
				s.SummaryByFilters(w, r, strings.Trim(strings.TrimSuffix(pa, "/count"), "/"))
			} else if strings.HasSuffix(pa, "/count/") {
				s.SummaryByFilters(w, r, strings.Trim(strings.TrimSuffix(pa, "/count/"), "/"))
			} else {
				s.SearchByFilters(w, r, strings.Trim(pa, "/"))
			}
			return
		}
	case "filters":
		switch r.Method {
		case "GET":
			if pa == "" || pa == "/" {
				s.ListFilterIDs(w, r)
			} else {
				s.ReadFilter(w, r, strings.Trim(pa, "/"))
			}
			return
		case "POST":
			if pa != "" || pa == "/" {
				w.WriteHeader(http.StatusMethodNotAllowed)
				w.Write([]byte("MethodNotAllowed"))
			} else {
				s.CreateFilter(w, r)
			}
			return
		case "DELETE":
			if pa == "" || pa == "/" {
				w.WriteHeader(http.StatusMethodNotAllowed)
				w.Write([]byte("MethodNotAllowed"))
			} else {
				s.DeleteFilter(w, r, strings.Trim(pa, "/"))
			}
			return
		case "PUT":
			if pa == "" || pa == "/" {
				w.WriteHeader(http.StatusMethodNotAllowed)
				w.Write([]byte("MethodNotAllowed"))
			} else {
				s.UpdateFilter(w, r, strings.Trim(pa, "/"))
			}
			return
		}

	case "syslogs":
		if r.Method == "POST" || r.Method == "PUT" {
			s.RecvSyslogs(w, r)
			return
		}
	case "raw":
		switch pa {
		case "count":
			s.Summary(w, r)
			return
		case "":
			s.Get(w, r)
			return
		default:
			http.DefaultServeMux.ServeHTTP(w, r)
			return
		}
	}
	if s.NoRoute == nil {
		http.DefaultServeMux.ServeHTTP(w, r)
	} else {
		s.NoRoute.ServeHTTP(w, r)
	}
}

func (s *Server) RenderText(w http.ResponseWriter, req *http.Request, code int, txt string) error {
	w.WriteHeader(code)
	_, e := w.Write([]byte(txt))
	return e
}

func (s *Server) RecvSyslogs(w http.ResponseWriter, req *http.Request) {
	bs, err := ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("read http body: %v", err), http.StatusInternalServerError)
		return
	}
	bs = bytes.TrimSpace(bs)
	if len(bs) == 0 {
		http.Error(w, "http body is empty", http.StatusInternalServerError)
		return
	}
	if bytes.HasPrefix(bs, []byte("[")) {
		var events []input.Event
		err := json.Unmarshal(bs, &events)
		if err != nil {
			http.Error(w, fmt.Sprintf("%v\r\n%s", err, bs), http.StatusInternalServerError)
			return
		}
		for idx := range events {
			s.c <- &events[idx]
		}
		return
	}

	if bytes.HasPrefix(bs, []byte("{")) {
		var evt input.Event
		err := json.Unmarshal(bs, &evt)
		if err != nil {
			http.Error(w, fmt.Sprintf("%v\r\n%s", err, bs), http.StatusInternalServerError)
			return
		}
		s.c <- &evt
		return
	}

	http.Error(w, fmt.Sprintf("http body is invalid event(s)\r\n%s", bs), http.StatusInternalServerError)
}

func (s *Server) Summary(w http.ResponseWriter, req *http.Request) {
	s.Search(w, req, false, func(req *bleve.SearchRequest, resp *bleve.SearchResult) error {
		return encodeJSON(w, resp.Total)
	})
}

func (s *Server) Get(w http.ResponseWriter, req *http.Request) {
	s.Search(w, req, true, func(req *bleve.SearchRequest, resp *bleve.SearchResult) error {
		var documents = make([]interface{}, 0, resp.Hits.Len())
		for _, doc := range resp.Hits {
			documents = append(documents, doc.Fields)
		}
		return encodeJSON(w, documents)
	})
}

func (s *Server) FieldDict(w http.ResponseWriter, req *http.Request, field string) {
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

func (s *Server) Fields(w http.ResponseWriter, req *http.Request) {
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

func (s *Server) timeRange(w http.ResponseWriter, req *http.Request,
	cb func(w http.ResponseWriter, req *http.Request, start, end time.Time)) {
	queryParams := req.URL.Query()

	var start, end time.Time

	startAt := queryParams.Get("start_at")
	if startAt != "" {
		start = service.ParseTime(startAt)
		if start.IsZero() {
			http.Error(w, "start_at("+startAt+") is invalid.", http.StatusBadRequest)
			return
		}
	}

	if endAt := queryParams.Get("end_at"); endAt != "" {
		end = service.ParseTime(endAt)
		if end.IsZero() {
			http.Error(w, "end_at("+endAt+") is invalid.", http.StatusBadRequest)
			return
		}
	}

	cb(w, req, start, end)
}

func (s *Server) Search(w http.ResponseWriter, req *http.Request, allFields bool, cb func(req *bleve.SearchRequest, resp *bleve.SearchResult) error) {
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

func (s *Server) SearchIn(w http.ResponseWriter, req *http.Request, searchRequest *bleve.SearchRequest, cb func(req *bleve.SearchRequest, resp *bleve.SearchResult) error) {
	queryParams := req.URL.Query()

	var start, end time.Time

	startAt := queryParams.Get("start_at")
	if startAt != "" {
		start = service.ParseTime(startAt)
		if start.IsZero() {
			http.Error(w, "start_at("+startAt+") is invalid.", http.StatusBadRequest)
			return
		}
	}

	if endAt := queryParams.Get("end_at"); endAt != "" {
		end = service.ParseTime(endAt)
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
func (s *Server) QueryHTML(w http.ResponseWriter, r *http.Request) {
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
func serveIndex(s *Server, w http.ResponseWriter, r *http.Request) error {
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
