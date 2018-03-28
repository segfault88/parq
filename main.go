package main

import (
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetReader"
	"github.com/xitongsys/parquet-go/ParquetWriter"
	"github.com/xitongsys/parquet-go/parquet"
)

type Student struct {
	FirstName string  `parquet:"name=firstName, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LastName  string  `parquet:"name=lastName, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Age       int32   `parquet:"name=age, type=INT32"`
	IID       string  `parquet:"name=iid, type=BYTE_ARRAY"`
	ID        int64   `parquet:"name=id, type=INT64"`
	Weight    float32 `parquet:"name=weight, type=FLOAT"`
	Sex       bool    `parquet:"name=sex, type=BOOLEAN"`
	Day       int32   `parquet:"name=day, type=DATE"`
	Ignored   int32   //without parquet tag and won't write
}

var (
	wordLength = 4
)

func getWords() []string {
	f, err := os.Open("/usr/share/dict/words")
	if err != nil {
		log.Fatalf("Couldn't open wors %s", err)
	}
	defer f.Close()
	data, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalf("Couldn't read words file")
	}

	out := []string{}

	words := strings.Split(string(data), "\n")
	for _, word := range words {
		if len(word) > wordLength {
			out = append(out, word)
		}
	}

	return out
}

func randomWord(words []string, usedWords []string) string {
	j := int(rand.Int31()) % len(words)
	word := words[j]

	// check that word hasn't already been used
	for _, used := range usedWords {
		if word == used {
			// word already used, try again
			return randomWord(words, usedWords)
		}
	}

	// word not already used
	return word
}

func main() {
	words := getWords()
	rand.Seed(int64(time.Now().Nanosecond()))

	var err error
	fw, err := ParquetFile.NewLocalFileWriter("flat.parquet")
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}

	//write
	pw, err := ParquetWriter.NewParquetWriter(fw, new(Student), 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	num := 65536
	for i := 0; i < num; i++ {
		u1 := uuid.Must(uuid.NewV4())
		u1.Bytes()

		stu := Student{
			FirstName: randomWord(words, []string{}),
			LastName:  randomWord(words, []string{}),
			Age:       rand.Int31() % 99,
			ID:        rand.Int63(),
			IID:       string(u1.Bytes()),
			Weight:    float32(50.0 + float32(i)*0.1),
			Sex:       bool(int(rand.Int31())%2 == 0),
			Day:       int32(time.Now().Unix() / 3600 / 24),
		}
		if err = pw.Write(stu); err != nil {
			log.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}
	log.Println("Write Finished")
	fw.Close()

	///read
	fr, err := ParquetFile.NewLocalFileReader("flat.parquet")
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err := ParquetReader.NewParquetReader(fr, new(Student), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	num = int(pr.GetNumRows())
	// for i := 0; i < num; i++ {
	stus := make([]Student, num)
	if err = pr.Read(&stus); err != nil {
		log.Println("Read error", err)
	}
	// log.Println(stus)
	_ = stus
	// }

	pr.ReadStop()
	fr.Close()

}
