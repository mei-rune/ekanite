package ekanite

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/document"
)

func Convert(pa string, delta time.Duration, create func(pa string) (Writer, error)) error {
	fi, err := os.Stat(pa)
	if err != nil {
		return fmt.Errorf("failed to access index at %s: %v", pa, err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("index %s path is not a directory", pa)
	}

	_, err = os.Stat(filepath.Join(pa, endTimeFileName))
	if err != nil {
		if os.IsNotExist(err) {
			names, err := ioutil.ReadDir(pa)
			if err != nil {
				return fmt.Errorf("failed to access index at %s: %v", pa, err)
			}

			for _, name := range names {
				if !name.IsDir() {
					fmt.Println("'" + filepath.Join(pa, name.Name()) + "' is skipped")
					continue
				}
				if strings.HasSuffix(name.Name(), ".new") {
					fmt.Println("'" + filepath.Join(pa, name.Name()) + "' is skipped")
					continue
				}

				if strings.HasSuffix(name.Name(), ".old") {
					fmt.Println("'" + filepath.Join(pa, name.Name()) + "' is skipped")
					continue
				}

				err := copyIndex(filepath.Join(pa, name.Name()), delta, create)
				if err != nil {
					return err
				}
			}
			return nil
		}
		return fmt.Errorf("failed to access index at %s: %v", pa, err)
	}

	return copyIndex(pa, delta, create)
}

func copyIndex(pa string, delta time.Duration, create func(pa string) (Writer, error)) error {
	names, err := listShards(pa)
	if err != nil {
		return err
	}

	dir := filepath.Dir(pa)
	newPath := filepath.Join(dir, filepath.Base(pa)+".new")

	if err := os.RemoveAll(newPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("ensure directory is empty: %s", err.Error())
	}
	fmt.Println("remove ", newPath)

	if err := os.MkdirAll(newPath, 0777); err != nil {
		return fmt.Errorf("ensure directory is exists: %s", err.Error())
	}

	for _, name := range names {
		fmt.Println("'" + name + "' is converting...")
		oldShard := NewShard(filepath.Join(pa, name))
		if err := oldShard.Open(); err != nil {
			return fmt.Errorf("old shard open fail: %s", err.Error())
		}

		//newShard := NewShard(filepath.Join(newPath, name))
		//if err := newShard.Open(); err != nil {
		newShard, err := create(filepath.Join(newPath, name))
		if err != nil {
			return fmt.Errorf("new shard open fail: %s", err.Error())
		}

		if err := copyShard(oldShard, newShard, delta); err != nil {
			return fmt.Errorf("copy shard fail: %s", err.Error())
		}
		oldShard.Close()
		newShard.Close()
	}

	bs, err := ioutil.ReadFile(filepath.Join(pa, endTimeFileName))
	if err != nil {
		return fmt.Errorf("read old endtime : %v", err)
	}

	tt, err := time.Parse(indexNameLayout, string(bs))
	if err != nil {
		return fmt.Errorf("read old endtime : %v", err)
	}

	err = ioutil.WriteFile(filepath.Join(newPath, endTimeFileName), []byte(tt.Format(indexNameLayout)), 0666)
	if err != nil {
		return fmt.Errorf("write new endtime : %v", err)
	}

	return nil
}

func NewShardWriter(pa string) (Writer, error) {
	newShard := NewShard(pa)
	if err := newShard.Open(); err != nil {
		return nil, err
	}

	return &shardWriter{
		newShard: newShard,
	}, nil
}

type Writer interface {
	Output(string, *document.Document, map[string]interface{}) error
	Close() error
}

type shardWriter struct {
	newShard *Shard
	batch    *bleve.Batch
}

func (sw *shardWriter) Output(id string, doc *document.Document, values map[string]interface{}) error {
	if sw.batch == nil {
		sw.batch = sw.newShard.b.NewBatch()
	}

	err := sw.batch.IndexAdvanced(doc)
	if err != nil {
		return fmt.Errorf("IndexAdvanced(%s) : %v", id, err)
	}
	return sw.batch.Index(id, values)
}

func (sw *shardWriter) Close() error {
	err := sw.newShard.b.Batch(sw.batch)
	if err != nil {
		return fmt.Errorf("Batch : %v", err)
	}
	return sw.newShard.Close()
}

func NewCsvWriter(out io.Writer) (Writer, error) {
	return &csvWriter{
		out: csv.NewWriter(out),
	}, nil
}

type csvWriter struct {
	out *csv.Writer
}

func (sw *csvWriter) Output(id string, doc *document.Document, values map[string]interface{}) error {
	return sw.out.Write([]string{
		id,
		fmt.Sprint(values["timestamp"]),
		fmt.Sprint(values["reception"]),
		fmt.Sprint(values["address"]),
		fmt.Sprint(values["message"]),
		fmt.Sprint(values["source"]),
	})
}

func (sw *csvWriter) Close() error {
	sw.out.Flush()
	return nil
}

func copyShard(oldShard *Shard, writer Writer, delta time.Duration) error {
	i, a, err := oldShard.b.Advanced()
	if err != nil {
		return fmt.Errorf("Advanced : %v", err)
	}
	if a != nil {
		defer a.Close()
	}
	// defer i.Close()

	r, err := i.Reader()
	if err != nil {
		return fmt.Errorf("Advanced.Reader() : %v", err)
	}
	defer r.Close()
	all, err := r.DocIDReaderAll()
	if err != nil {
		return fmt.Errorf("Advanced.Reader().All() : %v", err)
	}
	defer all.Close()

	//fmt.Println("count = ", all.Size())

	var docIDs = make([]string, 0, 1024)
	for {
		id, err := all.Next()
		if err != nil {
			return fmt.Errorf("Advanced.Reader().All().Next() : %v", err)
		}

		if id == nil {
			break
		}

		idStr, err := r.ExternalID(id)
		if err != nil {
			return fmt.Errorf("ExternalID(%s).Next() : %v", id, err)
		}

		docIDs = append(docIDs, idStr)
	}

	//b := newShard.b.NewBatch()

	fmt.Println("count =", len(docIDs))
	for idx, idStr := range docIDs {
		doc, err := oldShard.b.Document(idStr)
		if err != nil {
			return fmt.Errorf("Document(%s) : %v", idStr, err)
		}
		if doc == nil {
			return fmt.Errorf("Document(%s) : empty", idStr)
		}

		var values = map[string]interface{}{}
		for _, f := range doc.Fields {

			var value interface{}
			switch field := f.(type) {
			case *document.TextField:
				value = string(field.Value())
				if len(field.Value()) == 0 {
					if fieldName := f.Name(); fieldName == "structured_data" ||
						fieldName == "app_name" ||
						fieldName == "msg_id" ||
						fieldName == "proc_id" {
						break
					}
					panic(fmt.Errorf("field %s is empty", f.Name()))
				}
			case *document.NumericField:
				num, err := field.Number()
				if err != nil {
					panic(fmt.Errorf("field %s : %s", f.Name(), err))
				}
				value = int64(num)
			case *document.DateTimeField:
				t, err := field.DateTime()
				if err != nil {
					panic(fmt.Errorf("field %s : %s", f.Name(), err))
				}
				value = t.Add(delta)
			// case *document.DateTimeField:
			// 	t, _ := field.DateTime()
			// 	value = t
			case *document.BooleanField:
				b, err := field.Boolean()
				if err != nil {
					panic(fmt.Errorf("field %s : %s", f.Name(), err))
				}
				value = b
			default:
				panic(fmt.Errorf("%T %v", f, f))
			}

			if value != nil {
				values[f.Name()] = value
			}
		}

		for _, nm := range []string{
			"timestamp",
			"reception",
			"address",
			"message",
			"source",
		} {
			if _, ok := values[nm]; !ok {
				panic(errors.New("field '" + nm + "' is empty"))
			}
		}

		err = writer.Output(idStr, doc, values)
		// err = b.Index(idStr, values)
		if err != nil {
			return fmt.Errorf("IndexAdvanced(%d: %s) : %v", idx, idStr, err)
		}

		// fmt.Println(idStr, doc.GoString())
	}

	// err = newShard.b.Batch(b)
	// if err != nil {
	// 	return fmt.Errorf("Batch : %v", err)
	// }

	return nil
}
