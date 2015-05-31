package util

import (
    "os"
    "fmt"
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


func InitIOCtrl(buf_size int) *IOCtrl {
    tmp_chan := make(chan string, buf_size)
    ioctrl := &IOCtrl{Files: tmp_chan, Wg: new(sync.WaitGroup)}

    return ioctrl
}

func MergeTmpFiles(file_chan chan string) {
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

        for scanner.Scan() {
            line := scanner.Text()
            _, e := writer.WriteString(line+"\n")

            if e != nil { fmt.Print("[ERROR] in MergedTmpFiles:", e, "[TEXT]:", line)}
        }
        
        writer.Flush()

        fmt.Println("Merged", tmpf)
        
        f.Close()
        re := os.Remove(tmpf)

        if re != nil {
            fmt.Println("[ERROR] when deleting tmp file:", re)
        }
    }
}

func SaveRanking(name, qno string, ranking []DocScore, tmp_chan chan string) {
    rank_fmt := "%s Q0 %s %d %f Exp\n"
    save_file, e := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0777)
    writer := bufio.NewWriter(save_file)
    
    if e != nil {
        panic(e)
        //save_file, _ = os.Create("okapiTF_ranking.txt")
    }
    
    for i, r := range ranking {
        _, err := writer.WriteString(fmt.Sprintf(rank_fmt, qno, r.Id, i+1, r.Score))

        if err != nil{
            panic(err)
        }
    }
    writer.Flush()

    save_file.Close()
    tmp_chan <- name
}

func ReadQueries() ([]string, error) {
    qfile, err := os.Open("/ap_data/query_desc.51-100.short.txt")
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