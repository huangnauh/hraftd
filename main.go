package main

import (
    "bytes"
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "net/http"
    "os"
    "os/signal"

    "github.com/huangnauh/hraftd/http"
    "github.com/huangnauh/hraftd/store"
    "strings"
)

// Command line defaults
const (
    DefaultHTTPAddr = ":11000"
    DefaultRaftAddr = ":12000"
    DefaultSerfAddr = ":9094"
)

// Command line parameters
var httpAddr string
var raftAddr string
var joinAddr string
var serfAddr string
var smem string
var serfMembers []string
var idFlag *int64

func init() {
    flag.StringVar(&httpAddr, "haddr", DefaultHTTPAddr, "Set the HTTP bind address")
    flag.StringVar(&raftAddr, "raddr", DefaultRaftAddr, "Set Raft bind address")
    flag.StringVar(&joinAddr, "join", "", "Set join address, if any")
    flag.StringVar(&serfAddr, "saddr", DefaultSerfAddr, "Address for Serf to bind on")
    flag.StringVar(&smem, "smem", "", "List of existing Serf members")
    idFlag = flag.Int64("id", int64(1), "ID")
    flag.Usage = func() {
        fmt.Fprintf(os.Stderr, "Usage: %s [options] <raft-data-path> \n", os.Args[0])
        flag.PrintDefaults()
    }
}

func main() {
    flag.Parse()

    id := *idFlag
    log.Println("id: ", id)
    log.Println("serfM: ", smem)
    if strings.Contains(smem, ",") {
        serfMembers = strings.Split(smem, ",")
    } else if smem != "" {
        serfMembers = []string{smem}
    }

    log.Println("serfmembers: ", serfMembers)
    if flag.NArg() == 0 {
        fmt.Fprintf(os.Stderr, "No Raft storage directory specified\n")
        os.Exit(1)
    }

    // Ensure Raft storage exists.
    raftDir := flag.Arg(0)
    if raftDir == "" {
        fmt.Fprintf(os.Stderr, "No Raft storage directory specified\n")
        os.Exit(1)
    }
    os.MkdirAll(raftDir, 0700)

    s := store.New(id)
    s.RaftDir = raftDir
    s.RaftBind = raftAddr
    if err := s.Open(joinAddr == "" && smem == "", serfMembers, serfAddr); err != nil {
        log.Fatalf("failed to open store: %s", err.Error())
    }

    h := httpd.New(httpAddr, s)
    if err := h.Start(); err != nil {
        log.Fatalf("failed to start HTTP service: %s", err.Error())
    }

    // If join was specified, make the join request.
    if joinAddr != "" {
        if err := join(joinAddr, raftAddr); err != nil {
            log.Fatalf("failed to join node at %s: %s", joinAddr, err.Error())
        }
    }

    log.Println("hraft started successfully")

    terminate := make(chan os.Signal, 1)
    signal.Notify(terminate, os.Interrupt)
    <-terminate
    log.Println("hraftd exiting")
}

func join(joinAddr, raftAddr string) error {
    b, err := json.Marshal(map[string]string{"addr": raftAddr})
    if err != nil {
        return err
    }
    resp, err := http.Post(fmt.Sprintf("http://%s/join", joinAddr), "application-type/json", bytes.NewReader(b))
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    return nil
}

