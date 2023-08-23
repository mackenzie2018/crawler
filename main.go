package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

func GetHomeDir() (string, error) {
    homeDir, homeDirErr := os.UserHomeDir()
    if homeDirErr != nil {
        return "", fmt.Errorf("Failed to get user home directory: %v", homeDirErr)
    }

    return homeDir, nil
}

func CopyFile(from string, to string) (int64, error) {
    sourceFileStat, err := os.Stat(from)
    if err != nil {
        return 0, err
    }

    if !sourceFileStat.Mode().IsRegular() {
        return 0, fmt.Errorf("%s is not a regular file", from)
    }

    source, err := os.Open(from)
    if err != nil {
        return 0, err
    }
    defer source.Close()

    destination, err := os.Create(to)
    if err != nil {
        return 0, err
    }
    defer destination.Close()

    nBytes, err := io.Copy(destination, source)
    return nBytes, err
}

type FileMetadata struct {
    uid int64
    Name string
    Ext string
    ModDate time.Time
    IsDir bool
    Size int64
    Path string
    IsReg bool
}

type CopyJob struct {
    Source string
    Destination string
}

func ParseFileTypes(fileTypes string, sep string) map[string]bool {
    SplitFileTypeString := strings.Split(fileTypes, sep)
    TargetFileTypes := make(map[string]bool)
    for _, ft := range SplitFileTypeString {
        TargetFileTypes[strings.ToLower(ft)] = true
    }
    return TargetFileTypes
}

func CopyFileWorker(jobs <- chan CopyJob, errors chan <- error, wg *sync.WaitGroup) {
    for job := range jobs {
        log.Println("Copying ", job.Source, " to ", job.Destination)
        _, err := CopyFile(job.Source, job.Destination)
        if err != nil {
            errors <- err
        }
        wg.Done()
    }
}

func main() {
    homeDir, homeDirErr := GetHomeDir()
    if homeDirErr != nil {
        fmt.Println("Error: ", homeDirErr)
    }

    f, _ := os.Create("./crawler_log.txt")
    defer f.Close()
    log.SetOutput(f)
    log.Println("This log is written to crawler_log.txt")

    var RootDir string
    flag.StringVar(&RootDir, "rootDir", homeDir, "The root directory to crawl")
    var FileTypes string
    flag.StringVar(&FileTypes, "fileType", ".py", "The file types to find")
    var ToDir string
    flag.StringVar(&ToDir, "toDir", "/tmp/", "The directory to copy files into")
    var CopyFilesFlag bool
    flag.BoolVar(&CopyFilesFlag, "CopyFilesFlag", false, "Copy files into ToDir directory")
    var EchoFilesFlag bool
    flag.BoolVar(&EchoFilesFlag, "EchoFilesFlag", true, "Print results to stdout")
    flag.Parse()

    log.Printf("crawler called\n")
    log.Printf("Parsing %v\n", RootDir)
    log.Printf("Looking for files of type: %v\n", FileTypes)
    log.Printf("Output directory: %v\n", ToDir)
    log.Printf("Copy files? %v\n", CopyFilesFlag)
    log.Printf("Echo files? %v\n", EchoFilesFlag)

    ParsedFileTypes := ParseFileTypes(FileTypes, ",")

    if EchoFilesFlag {
        fmt.Printf("UID\tName\tExtension\tModDate\tIsDir\tSize(B)\tFilePath\tIsRegularfile\n")
    }

    CopyJobs := make([]CopyJob, 0)
    Count := 0
    err := filepath.Walk(RootDir, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }

        data := FileMetadata{
            uid: int64(Count),
            Name: info.Name(),
            Ext: filepath.Ext(path),
            ModDate: info.ModTime(),
            IsDir: info.IsDir(),
            Size: info.Size(),
            Path: path,
            IsReg: info.Mode().IsRegular(),
        }



        _, IsOfTargetFileType := ParsedFileTypes[strings.ToLower(data.Ext)]
        if !data.IsDir && IsOfTargetFileType {
            if EchoFilesFlag {
                fmt.Printf("%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\n",
                    data.uid,
                    data.Name, 
                    data.Ext, 
                    data.ModDate,
                    data.IsDir,
                    data.Size,
                    data.Path,
                    data.IsReg,
                )
            }

            if CopyFilesFlag {
                job := CopyJob{
                    data.Path,
                    filepath.Join(ToDir, fmt.Sprint(data.uid) + "_" + data.Name),
                }
                CopyJobs = append(CopyJobs, job)
                Count += 1
            }
        }

        return nil
    })

    jobs := make(chan CopyJob, len(CopyJobs))
    jobErrors := make(chan error, len(CopyJobs))
    var wg sync.WaitGroup

    for i := 0; i < 5; i++ {
        go CopyFileWorker(jobs, jobErrors, &wg)
    }

    for _, job := range CopyJobs {
        jobs <- job
        wg.Add(1)
    }
    close(jobs)

    go func() {
        for err := range jobErrors {
            log.Println(err)
        }
    }()

    wg.Wait()
    close(jobErrors)

    if err != nil {
        log.Printf("Error walking the directory: %v\n", err)
    }
}
