package bleve_sego

import (
	"errors"
	"path/filepath"
	"strings"
	"sync"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/registry"
	"github.com/huichen/sego"
)

var RootDir string

func init() {
	registry.RegisterAnalyzer("sego", analyzerConstructor)
	registry.RegisterTokenizer("sego", tokenizerConstructor)
}

type SegoTokenizer struct {
	tker sego.Segmenter
}

func (s *SegoTokenizer) loadDictory(dict string) {
	if RootDir != "" && !filepath.IsAbs(dict) {
		dict = filepath.Join(RootDir, dict)
	}
	s.tker.LoadDictionary(dict)
}

func (s *SegoTokenizer) Tokenize(sentence []byte) analysis.TokenStream {
	result := make(analysis.TokenStream, 0)
	words := s.tker.Segment(sentence)
	for pos, word := range words {
		word.Token().Text()
		token := analysis.Token{
			Start:    word.Start(),
			End:      word.End(),
			Position: pos + 1,
			Term:     []byte(word.Token().Text()),
			Type:     analysis.Ideographic,
		}
		result = append(result, &token)
	}
	return result
}

var (
	tokenizerLock  sync.Mutex
	tokenizerCache = map[string]analysis.Tokenizer{}
)

func tokenizerConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.Tokenizer, error) {
	dictpath, ok := config["dictpath"].(string)
	if !ok {
		return nil, errors.New("config dictpath not found")
	}

	dictpath = filepath.ToSlash(dictpath)

	if strings.HasPrefix(dictpath, "D:/609_monitorsoft/") {
		dictpath = strings.TrimPrefix(dictpath, "D:/609_monitorsoft/")
	}
	if strings.HasPrefix(dictpath, "C:/Program Files/hengwei/") {
		dictpath = strings.TrimPrefix(dictpath, "C:/Program Files/hengwei/")
	}
	if strings.HasPrefix(dictpath, "D:/Program Files/hengwei/") {
		dictpath = strings.TrimPrefix(dictpath, "D:/Program Files/hengwei/")
	}
	if strings.HasPrefix(dictpath, "d:/Program Files/hengwei/") {
		dictpath = strings.TrimPrefix(dictpath, "d:/Program Files/hengwei/")
	}
	if strings.HasPrefix(dictpath, "D:/hengwei/") {
		dictpath = strings.TrimPrefix(dictpath, "D:/hengwei/")
	}
	if strings.HasPrefix(dictpath, "d:/hengwei/") {
		dictpath = strings.TrimPrefix(dictpath, "d:/hengwei/")
	}

	tokenizerLock.Lock()
	defer tokenizerLock.Unlock()
	if old := tokenizerCache[dictpath]; old != nil {
		return old, nil
	}

	tokenizer := &SegoTokenizer{}
	tokenizer.loadDictory(dictpath)
	tokenizerCache[dictpath] = tokenizer
	return tokenizer, nil
}

type SegoAnalyzer struct{}

func analyzerConstructor(config map[string]interface{}, cache *registry.Cache) (*analysis.Analyzer, error) {
	tokenizerName, ok := config["tokenizer"].(string)
	if !ok {
		return nil, errors.New("must specify tokenizer")
	}
	tokenizer, err := cache.TokenizerNamed(tokenizerName)
	if err != nil {
		return nil, err
	}
	alz := &analysis.Analyzer{
		Tokenizer: tokenizer,
	}
	return alz, nil
}
