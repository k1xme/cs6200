package util

import (
    "os"
    "fmt"
    "log"
    "strings"
    "bufio"
    "sync"
)

/*
* NOTE: When using WaitGroup, you have to pass its pointer
* to the called function. Otherwise will incur livelock.
*/
type IOCtrl struct {
    Files chan string
    Wg *sync.WaitGroup
}

func NewSema(size int) chan bool {
    sema := make(chan bool, size)

    // init the sema by @size.
    for i := 0; i < size; i++ {
        sema <- true
    }

    return  sema   
}

func InitIOCtrl(buf_size int) *IOCtrl {
    tmp_chan := make(chan string, buf_size)
    ioctrl := &IOCtrl{Files: tmp_chan, Wg: new(sync.WaitGroup)}

    return ioctrl
}

func MergeTmpFiles(file_chan chan string, wg *sync.WaitGroup) {
    defer wg.Done()

    file_map := make(map[string]*os.File)

    for _, model := range models {
        fname := fmt.Sprintf("%s_ranking.txt", model)
        file_map[model], _ = os.Create(fname)
    }

    for tmpf := range file_chan {
        f, _ := os.Open(tmpf)
        
        scanner := bufio.NewScanner(f)
        scanner.Split(bufio.ScanLines)
        
        model := strings.Split(tmpf, ".")[0]
        writer := bufio.NewWriter(file_map[model])
        if writer == nil {
            log.Fatal("[ERROR] Cannot find merged file:", "[Model]:", model)
        }
        for scanner.Scan() {
            line := scanner.Text()
            _, e := writer.WriteString(string(line+"\n"))

            if e != nil { 
                log.Println("[ERROR] in MergedTmpFiles:", "[Model]:", model, "[LINE]", line)
            }
        }
        
        writer.Flush()

        Print("-- Merged ", tmpf)

        f.Close()
        re := os.Remove(tmpf)

        if re != nil {
            log.Println("[ERROR] when deleting tmp file:", re)
        }
    }
}

func SaveRanking(name, qno string, ranking []DocScore, tmp_chan chan string) {
    rank_fmt := "%s Q0 %s %d %f Exp\n"
    save_file, e := os.Create(name)
    writer := bufio.NewWriter(save_file)
    
    if e != nil {
        panic(e)
    }
    
    for i, r := range ranking {
        _, err := writer.WriteString(fmt.Sprintf(rank_fmt, qno, r.Id, i+1, r.Score))

        if err != nil{
            panic(err)
        }
    }

    // Ending operations
    writer.Flush()
    save_file.Close()
    tmp_chan <- name
}

func ReadQueries(query_path string) ([]string, error) {
    qfile, err := os.Open(query_path)
    reader := bufio.NewReader(qfile)

    if err != nil {
        return nil, err
    }

    var (
        line string
        queries []string
        )

    for line_buf, notdone, err := reader.ReadLine(); err == nil;
        line_buf, notdone, err = reader.ReadLine() {
            line += string(line_buf)
            if notdone || line == "" {
                continue
            } else {
                queries = append(queries, line)
                line = ""
            }
        }

    return queries, err
}

func Print(s ...interface{}) {
    fmt.Print("                           \r")
    fmt.Print(s...)
    fmt.Print("\r")
}

func NewTmpWriter(name string) *bufio.Writer {
    save_file, e := os.Create(name)
    writer := bufio.NewWriter(save_file)

    if e != nil {
        panic(e)
    }

    return writer
}

func NewTmpReader(name string) *bufio.Reader {
    f, e := os.Open(name)
    HandleError(e)
    reader := bufio.NewReader(f)
    return reader
}

func CleanTmps(file ...*os.File) error {
    for _, f := range file {
        if f == nil {continue}
        f.Close()
        err := os.Remove(f.Name())
        HandleError(err)
        err = os.Remove(f.Name()+".catalog")
        HandleError(err)
    }
    return nil
}