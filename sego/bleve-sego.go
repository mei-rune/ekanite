package bleve_sego

import (
	"errors"
	"sync"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/registry"
	"github.com/huichen/sego"
)

func init() {
	registry.RegisterAnalyzer("sego", analyzerConstructor)
	registry.RegisterTokenizer("sego", tokenizerConstructor)
}

type SegoTokenizer struct {
	tker sego.Segmenter
}

func (s *SegoTokenizer) loadDictory(dict string) {
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
