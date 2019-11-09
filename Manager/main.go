package main

import (
    "os"
    "io"
    "fmt"
    "time"
    "flag"
    "bufio"
    "reflect"
    "context"
    "os/exec"
    "strconv"
    "strings"
    "database/sql"
    "path/filepath"
    "gopkg.in/pipe.v2"
    "google.golang.org/grpc"
    "github.com/akkuman/parseConfig"
    _ "github.com/go-sql-driver/mysql"
    "v2ray.com/core/app/proxyman/command"
    statscmd "v2ray.com/core/app/stats/command"
    "v2ray.com/core/common/protocol"
    "v2ray.com/core/common/serial"
    "v2ray.com/core/proxy/vmess"
)

type TrafficInfo struct {
    Up, Down int64
}

const (
    UplinkFormat   = "user>>>%s>>>traffic>>>uplink"
    DownlinkFormat = "user>>>%s>>>traffic>>>downlink"
)

var V2_users = make(map[int]map[string]string)
var check_time = 1
var config parseConfig.Config
var Mysql_Host string
var Mysql_Port string
var Mysql_User string
var Mysql_Password string
var Mysql_Db string
var Server_ID string
var Mysql_TLS string
var Mysql_MaxOpenConns int
var Mysql_MaxIdleConns int
var client command.HandlerServiceClient
var statsClient statscmd.StatsServiceClient
var V2rayClientAddr string
var V2rayTag string
var V2RayPid int
var EmailPostfix string
var Level uint32
var AlterID uint32
var Mysql_Server string
var V2config string
var CheckRate int
var M2Version = "Beggar - V0.1.3"
var Mydb *sql.DB

var (
    cfg = flag.String("cfg", "", "Config file for Manager.")
    V2cfg = flag.String("V2cfg", "", "Config file for V2Ray.")
    MVersion = flag.Bool("version", false, "Show version for Manager.")
)

func main() {
    flag.Parse()
    if(*MVersion){
        serviceLogger(fmt.Sprintf("V2Ray Manager By Suzume(%s)", M2Version), 0)
        os.Exit(0)
    }
    serviceLogger(fmt.Sprintf("V2Ray Manager By Suzume(%s)", M2Version), 0)
    start()
}

func initMysql() {
    Mydb, _ = sql.Open("mysql", Mysql_Server)
    Mydb.SetMaxOpenConns(Mysql_MaxOpenConns)
    Mydb.SetMaxIdleConns(Mysql_MaxIdleConns)
}

func start(){
    if *cfg != "" {
        serviceLogger(fmt.Sprintf("Loading Config: %s\n", *cfg), 0)
        config = parseConfig.New(*cfg)
    }else{
        config = parseConfig.New("config.json")
    }
    var err = checkConfig(config)
    if err != nil {
        serviceLogger(fmt.Sprintf("%v", err), 31)
        os.Exit(3)
    }
    initConfig(config)
    killV2Ray()
    serviceLogger(fmt.Sprintf("Starting V2Ray! AlterID: %d, Level: %d", AlterID, Level), 0) 
    go startV2Ray()
    timeSleep(4)
    cc, err := grpc.Dial(V2rayClientAddr, grpc.WithInsecure())
    if err != nil {
        serviceLogger(fmt.Sprintf("%v", err), 31)
        os.Exit(3)
    }
    client = command.NewHandlerServiceClient(cc)
    client = client
    statsClient = statscmd.NewStatsServiceClient(cc)
    statsClient = statsClient
    for{
        err := testAddUser()
        if err == nil {
            break
        }
        timeSleep(1)
    }
    serviceLogger(fmt.Sprintf("Connecting to Mysql Database!(TLS:%s)", Mysql_TLS), 0)
    serviceLogger(fmt.Sprintf("Mysql Setting: MaxOpen(%v), MaxIdle(%v)", Mysql_MaxOpenConns, Mysql_MaxIdleConns), 0)
    //fmt.Printf("Connecting to Mysql: %s:%s, User: %s, password:%s, Using Database: %s\n", Mysql_Host, Mysql_Port, Mysql_User, Mysql_Password, Mysql_Db)
    Mysql_Server = ""
    Mysql_Server += Mysql_User + ":" + Mysql_Password + "@tcp(" + Mysql_Host + ":" + Mysql_Port + ")/" + Mysql_Db + "?charset=utf8&multiStatements=true&tls=" + Mysql_TLS
    initMysql()
    Run()
}

func checkConfig(config parseConfig.Config) error{
    var CheckItems = make(map[string]string)
    CheckItems["Mysql_Host"] = "string"
    CheckItems["Mysql_Port"] = "float64"
    CheckItems["Mysql_User"] = "string"
    CheckItems["Mysql_Password"] = "string"
    CheckItems["Mysql_Db"] = "string"
    CheckItems["Server_ID"] = "float64"
    CheckItems["Mysql_TLS"] = "string"
    CheckItems["Mysql_MaxOpenConns"] = "float64"
    CheckItems["Mysql_MaxIdleConns"] = "float64"
    CheckItems["V2rayClientAddr"] = "string"
    CheckItems["V2rayTag"] = "string"
    CheckItems["Email"] = "string"
    CheckItems["AlterID"] = "float64"
    CheckItems["Level"] = "float64"
    CheckItems["CheckRate"] = "float64"
    for k, v := range CheckItems{
        var CheI = config.Get(k)
        if(CheI == nil){
            fmt.Printf("%c[1;0;31mConfig Missing: %s%c[0m\n", 0x1B, k, 0x1B)
            fmt.Printf("%c[1;0;31mPlease Check Your Config File!%c[0m\n", 0x1B, 0x1B)
            os.Exit(0)
        }else{
            if(fmt.Sprintf("%v", reflect.TypeOf(CheI)) != v){
                fmt.Printf("%c[1;0;31mWrong Config Item: %s%c[0m\n", 0x1B, k, 0x1B)
                fmt.Printf("%c[1;0;31mNeed: %s, Give: %s%c[0m\n", 0x1B, v, reflect.TypeOf(CheI), 0x1B)
                fmt.Printf("%c[1;0;31mPlease Check Your Config File!%c[0m\n", 0x1B, 0x1B)
                os.Exit(0)
            }
        }
    }
    return nil
}

func initConfig(config parseConfig.Config){
    Mysql_Host = config.Get("Mysql_Host").(string)
    var Mysql_Portt = config.Get("Mysql_Port")
    Mysql_Port = strconv.Itoa(int(Mysql_Portt.(float64)))
    Mysql_User = config.Get("Mysql_User").(string)
    Mysql_Password = config.Get("Mysql_Password").(string)
    Mysql_Db = config.Get("Mysql_Db").(string)
    Server_ID = strconv.Itoa(int(config.Get("Server_ID").(float64)))
    Mysql_TLS = config.Get("Mysql_TLS").(string)
    Mysql_MaxOpenConns = int(config.Get("Mysql_MaxOpenConns").(float64))
    Mysql_MaxIdleConns = int(config.Get("Mysql_MaxIdleConns").(float64))
    V2rayClientAddr = config.Get("V2rayClientAddr").(string)
    V2rayTag = config.Get("V2rayTag").(string)
    EmailPostfix = config.Get("Email").(string)
    Level = uint32(int(config.Get("Level").(float64)))
    AlterID = uint32(int(config.Get("AlterID").(float64)))
    CheckRate = int(config.Get("CheckRate").(float64))
}

func Run() error {
    err := Mydb.Ping()
    if err != nil {
        serviceLogger(fmt.Sprintf("%v", err), 31)
        os.Exit(3)
    }
    V2_userss, err := selectDB()
    if(err != nil){
        serviceLogger(fmt.Sprintf("%v", err), 31)
        os.Exit(3)
    }
    V2_users = V2_userss
    InitUsers()
    ch := make(chan string, 1)
    serviceLogger(fmt.Sprintf("Loaded CheckRate : %d Second", int(CheckRate)), 32)
    for {
        timeSleep(CheckRate)
        serviceLogger(fmt.Sprintf("Start Checking, Round %d", int(check_time)), 0)   
        go func() {
            roundCheck()
            ch <- "done"
        }()
        select {
        case <-ch:
            serviceLogger(fmt.Sprintf("Task(%d) Is Done!!!!!", int(check_time)), 32)
        case <-time.After(time.Duration(CheckRate - 1) * time.Second):
            serviceLogger(fmt.Sprintf("Task(%d) Is Timeout!!!!!", int(check_time)), 31)
        }
        check_time = check_time + 1
    }
    return nil
}

func roundCheck(){
    checkV2RayAlive()
    CheckUsers(check_time)
}

func checkV2RayAlive(){
    if(!checkV2ray(V2RayPid)){
        serviceLogger("Restarting V2Ray!", 31)
        killV2Ray()
        pid, err := runV2Ray(V2config)
        if( err != nil){
            serviceLogger(fmt.Sprintf("RunV2Ray Error! Error:%s ",err), 31)
        }else{
            V2RayPid = pid
            serviceLogger(fmt.Sprintf("Restarted V2Ray!Pid:%d", pid), 32)
            for{
                err := testAddUser()
                if err == nil {
                    break
                }
                timeSleep(1)
            }
            serviceLogger("Start Initing Users!", 32)
            InitUsers()
        }
    }
}

func checkV2ray(pid int) bool{
    p := pipe.Line(
        pipe.Exec("ps", "aux"),
        pipe.Exec("grep", "-v", "grep"),
        pipe.Exec("grep", strconv.Itoa(pid)),
    )
    output, err := pipe.CombinedOutputTimeout(p, time.Duration(2) * time.Second)
    if err != nil {
        serviceLogger(fmt.Sprintf("Check - Search Failed(Prehaps no V2Ray Running), %s!", err), 31)
    }else{
        total := string(output)
        if(strings.Contains(total, "/usr/bin/v2ray/v2ray")){
            return true
        }
        return false
    }
    return false
}

func startV2Ray(){
    path := getCurrentPath()
    var v2config = path + "/v2ray.json"
    if *V2cfg != "" {
        serviceLogger(fmt.Sprintf("Loading V2Ray Config: %s", *V2cfg), 0)
        v2config = *V2cfg
    }else{
        serviceLogger(fmt.Sprintf("You are now Running in: %s", path), 0)
    }
    V2config = v2config
    pid, err := runV2Ray(V2config)
    if err != nil {
        os.Exit(1)
    }
    var alive = checkV2ray(pid)
    if(alive){
        serviceLogger(fmt.Sprintf("Started V2Ray! Pid:%d", pid), 32)
        V2RayPid = pid
    }else{
        serviceLogger("Cannot Start V2Ray!", 31)
        os.Exit(1)
    }
}

func runV2Ray(v2config string) (int, error){
    cmd := exec.Command("/usr/bin/v2ray/v2ray", "-config", v2config)
    ppReader, err := cmd.StdoutPipe()
    defer ppReader.Close()
    var bufReader = bufio.NewReader(ppReader)
    if err != nil {
        serviceLogger(fmt.Sprintf("Create cmd stdoutpipe failed, Error: %s!", err), 31)
        return 0, err
    }
    err = cmd.Start()
    if err != nil {
        serviceLogger(fmt.Sprintf("Cannot start V2Ray, Error: %s!n",err), 31)
        return 0, err
    }
    go func() {
        var buffer []byte = make([]byte, 4096)
        for {
            n, err := bufReader.Read(buffer)
            if err != nil {
                if err == io.EOF {
                    serviceLogger(fmt.Sprintf("ERROR: %s!", "pipi has Closed"), 31)
                    break
                } else {
                    break
                }
            }
            fmt.Print(string(buffer[:n]))
        }
    }()
    timeSleep(3)
    go waitCmd(cmd)
    return cmd.Process.Pid, nil
}

func waitCmd(cmd *exec.Cmd){
    cmd.Wait()
}

func killV2Ray(){
    p := pipe.Line(
        pipe.Exec("ps", "-ef"),
        pipe.Exec("grep", "-v", "grep"),
        pipe.Exec("grep", "-i", "/usr/bin/v2ray/v2ray"),
        pipe.Exec("awk", "{print $2}"),
    )
    output, err := pipe.CombinedOutputTimeout(p, time.Duration(5) * time.Second)
    if err != nil {
        serviceLogger(fmt.Sprintf("Kill - Search Failed(Prehaps no V2Ray Running), %s!", err), 31)
    }else{
        total := string(output)
        if(total != ""){
            total = strings.Replace(total, "\n", "", -1)
            intotal, err := strconv.Atoi(total)
            if err != nil {
                serviceLogger(fmt.Sprintf("Error: %s!", fmt.Sprint(err)), 31)
            }else{
                proc, err := os.FindProcess(intotal)
                if err != nil {
                    serviceLogger(fmt.Sprintf("Error: %s!", fmt.Sprint(err)), 31)
                }
                proc.Kill()
                serviceLogger(fmt.Sprintf("Killed V2Ray(%s)", total), 32) 
            }
        }else{
            serviceLogger("No V2Ray Running", 32) 
        }
    }
}

func selectDB() (map[int]map[string]string, error){
    var dbusersget = make(map[int]map[string]string)
    //查询节点
    var node_speedlimit float64
    var singleMode string
    err := Mydb.QueryRow("SELECT `node_speedlimit`,`singleMode` FROM server where `id`=" + Server_ID +" AND (`node_bandwidth`<`node_bandwidth_limit` OR `node_bandwidth_limit`=0)").Scan(&node_speedlimit,&singleMode)
    if err != nil{
        serviceLogger(fmt.Sprintf("Mysql Error Node: %s", err), 31)
        return dbusersget, nil
    }
    
    //查询用户
    rows, err := Mydb.Query("SELECT id, uuid, t, u, d, accountId, port, transfer_enable FROM ssr_user WHERE uuid is not null and serverId=" + Server_ID)
    if err != nil {
        serviceLogger(fmt.Sprintf("Mysql Error: %s", err), 31)
    }else{
        for rows.Next() {
            var id int
            var uuid string
            var t int
            var u int
            var d int
            var accountId int
            var port int
            var transfer_enable int
            err = rows.Scan(&id, &uuid, &t, &u, &d, &accountId, &port, &transfer_enable)
            checkErr(err)
            usermap := make(map[string]string)
            usermap["id"] = strconv.Itoa(id)
            usermap["uuid"] = strings.ToUpper(uuid)
            usermap["t"] = strconv.Itoa(t)
            usermap["u"] = strconv.Itoa(u)
            usermap["d"] = strconv.Itoa(d)            
            usermap["accountId"] = strconv.Itoa(accountId)
            usermap["port"] = strconv.Itoa(port)
            usermap["transfer_enable"] = strconv.Itoa(transfer_enable)
            dbusersget[id] = usermap
        }
        rows.Close()
        return dbusersget, nil
    }
    return dbusersget, err
}

func InitUsers(){
    for k, v := range V2_users{
        var emails = string(v["id"]) + EmailPostfix
        var uuids = string(v["uuid"])
        var ids = k
        var userr = &VUser{
            Email:     emails,
            UUID:      uuids,
            AlterID:   AlterID,
            Level:     Level,
            ID:        ids,
        }
        u, err := strconv.Atoi(v["u"])
        checkErr(err)
        d, err := strconv.Atoi(v["d"])
        checkErr(err)
        transfer_enable, err := strconv.Atoi(v["transfer_enable"])
        checkErr(err)
        var sum = u + d
        if(sum > transfer_enable){
            delete(V2_users,k)
        }else{
            addUser(userr)
        }
    }
}

func CheckUsers(mcheck_time int){
    newusers, err := selectDB()
    if(err != nil){
        serviceLogger("Skipped Round Check!", 33)
    }else{
        var umymap = make(map[string]map[string]string)
        for k, v := range newusers{
            if(mcheck_time != check_time){
                break
            }
            var emails = string(v["id"]) + EmailPostfix
            var uuids = string(v["uuid"])
            var ids = k
            var userr = &VUser{
                Email:     emails,
                UUID:      uuids,
                AlterID:   AlterID,
                Level:     Level,
                ID:        ids,
            }
            u, err := strconv.Atoi(v["u"])
            checkErr(err)
            d, err := strconv.Atoi(v["d"])
            checkErr(err)
            transfer_enable, err := strconv.Atoi(v["transfer_enable"])
            checkErr(err)
            var sum = u + d
            if(sum > transfer_enable){
                delete(newusers,k)
            }else{
                if _, ok := V2_users[k]; ok {
                    usertraffic := GetUserTrafficAndReset(userr)
                    serviceLogger(fmt.Sprintf("User(%s): U: %d, D: %d", emails, usertraffic.Up, usertraffic.Down), 0)
                    reti := TrafficInfo{}
                    reti.Up = usertraffic.Up
                    reti.Down = usertraffic.Down
                    if(reti.Up != 0 || reti.Down != 0){
                        usermap := make(map[string]string)
                        usermap["id"] = v["id"]
                        usermap["t"] = strconv.Itoa(int(time.Now().Unix()))
                        usermap["u"] = strconv.Itoa(int(reti.Up))
                        usermap["d"] = strconv.Itoa(int(reti.Down))
                        usermap["accountId"] = v["accountId"]
                        usermap["port"] = v["port"]
                        umymap[v["id"]] = usermap    
                    }
                }else{
                    addUser(userr)
                }
            }
        }
        makeUpdateQueue(umymap)
        for k, v := range V2_users{
            if(mcheck_time != check_time){
                break
            }
            var emails = string(v["id"]) + EmailPostfix
            var uuids = string(v["uuid"])
            var ids = k
            var userr = &VUser{
                Email:     emails,
                UUID:      uuids,
                AlterID:   AlterID,
                Level:     Level,
                ID:        ids,
            }
            if _, ok := newusers[k]; ok {
                if(uuids != newusers[k]["uuid"]){
                    removeUser(userr)
                    serviceLogger(fmt.Sprintf("User(%s): UUID(%s)→(%s)", emails, uuids, newusers[k]["uuid"]), 34)
                    var nuserr = &VUser{
                        Email:     emails,
                        UUID:      newusers[k]["uuid"],
                        AlterID:   AlterID,
                        Level:     Level,
                        ID:        ids,
                    }
                    addUser(nuserr)
                }
            }else{
                removeUser(userr)
            }
        }
        V2_users = newusers
    }
}

func makeUpdateQueue(umymap map[string]map[string]string){
    if(len(umymap) > 0){
        var ids = ""
        var us = ""
        var ds = ""
        var ts = ""
        var flow = 0
        var save_flow = "INSERT INTO `saveFlow`(`id`, `accountId`, `port`, `flow`, `time`) VALUES(1,1,1,1,1)"
        for k, v := range umymap{
            ids = ids + k + ","
            us = us + "WHEN " + v["id"] + " THEN u + " + v["u"] + "\n"
            ds = ds + "WHEN " + v["id"] + " THEN d + " + v["d"] + "\n"
            ts = ts + "WHEN " + v["id"] + " THEN " + v["t"] + "\n"
            d,errord := strconv.Atoi(v["d"])
            u,erroru := strconv.Atoi(v["u"])
            if errord != nil{
                fmt.Println("字符串转换成整数失败")
            }
            if erroru != nil{
                fmt.Println("字符串转换成整数失败")
            }
            flow = flow + d + u;
            save_flow = save_flow + fmt.Sprintf(", (%s,%s,%s,%v,%v)",Server_ID,v["accountId"],v["port"],d+u,time.Now().UnixNano() / 1e6)
            //save_flow = save_flow + fmt.Sprintf(", (%s,%s,%s,%v,%v)",Server_ID,v["accountId"],v["port"],d+u+1,time.Now().UnixNano() / 1e6)
        }
        ids = strings.TrimSuffix(ids, ",")
        //var umysqlp = "UPDATE ssr_user SET u = CASE id\n" + us + "END,\nd = CASE id\n" + ds + "END,\nt = CASE id\n" + ts + "END\nWHERE id IN(" + ids + ")"
        //serviceLogger(save_flow + ";" + umysqlp, 31)

        rows, err := Mydb.Exec(save_flow)
        if err != nil {
            serviceLogger(fmt.Sprintf("Update Failed: %s", err), 31)
        }else{
            rowCount, err := rows.RowsAffected()
            if err != nil{
                serviceLogger(fmt.Sprintf("Update Failed: %s", err), 31)
            }
            serviceLogger(fmt.Sprintf("Mysql Update Success, Affected %v Rows", int(rowCount)), 32)
        }
        //记录节点流量
        result,err := Mydb.Exec("UPDATE `server` SET `node_heartbeat`=unix_timestamp(),`node_bandwidth`=`node_bandwidth`+ ? WHERE `id` = ?",flow,Server_ID)
        if err != nil{
            serviceLogger(fmt.Sprintf("Update Failed: %s", err), 31)
            return
        }else{
            rowAffected, err := result.RowsAffected()
            if err != nil{
                serviceLogger(fmt.Sprintf("Update Server Failed: %s", err), 31)
            }
            serviceLogger(fmt.Sprintf("Mysql Update Server Success, Affected %v Rows", int(rowAffected)), 32)
        }
    }else{
        serviceLogger("Skipped Mysql Update(No Active Up And Down)", 32)
    }
}

func addUser(u *VUser) {
    var ctx = context.Background();
    resp, err := client.AlterInbound(ctx, &command.AlterInboundRequest{
        Tag: V2rayTag,
        Operation: serial.ToTypedMessage(&command.AddUserOperation{
            User: &protocol.User{
                Level: u.GetLevel(),
                Email: u.GetEmail(),
                Account: serial.ToTypedMessage(&vmess.Account{
                    Id:               u.GetUUID(),
                    AlterId:          u.GetAlterID(),
                    SecuritySettings: &protocol.SecurityConfig{Type: protocol.SecurityType_AUTO},
                }),
            },
        }),
    })
    if err != nil {
        serviceLogger(fmt.Sprintf("Failed to add Email: %s, Error:%v", u.GetEmail(), err), 31)
    } else {
        resp = resp
        serviceLogger(fmt.Sprintf("Added Email: %s, UUID: %s", u.GetEmail(), u.GetUUID()), 0)
    }
}

func testAddUser() error{
    var emaill = "test" + EmailPostfix
    var ctx = context.Background();
    var u = &VUser{
        Email:     emaill,
        UUID:      "3E187519-A207-4861-A589-2FE460E316CD",
        AlterID:   AlterID,
        Level:     Level,
        ID:        0,
    }
    resp, err := client.AlterInbound(ctx, &command.AlterInboundRequest{
        Tag: V2rayTag,
        Operation: serial.ToTypedMessage(&command.AddUserOperation{
            User: &protocol.User{
                Level: u.GetLevel(),
                Email: u.GetEmail(),
                Account: serial.ToTypedMessage(&vmess.Account{
                    Id:               u.GetUUID(),
                    AlterId:          u.GetAlterID(),
                    SecuritySettings: &protocol.SecurityConfig{Type: protocol.SecurityType_AUTO},
                }),
            },
        }),
    })
    if err != nil {
        serviceLogger(fmt.Sprintf("Failed to add Test User: %s, Please check V2Ray daemon!", u.GetEmail()), 31)
        return err
    } else {
        resp = resp
        serviceLogger("Connected To V2Ray Successfully!", 32)
        testRemoveUser(u)
    }
    return nil
}

func removeUser(u *VUser) {
    var ctx = context.Background();
    resp, err := client.AlterInbound(ctx, &command.AlterInboundRequest{
        Tag: V2rayTag,
        Operation: serial.ToTypedMessage(&command.RemoveUserOperation{
            Email: u.GetEmail(),
        }),
    })
    if err != nil {
        serviceLogger(fmt.Sprintf("Failed to Remove Email:%s, Error: %v", u.GetEmail(), err), 31)
    } else {
        resp = resp
        serviceLogger(fmt.Sprintf("Removed Email: %s, UUID: %s", u.GetEmail(), u.GetUUID()), 0)
    }
}

func testRemoveUser(u *VUser) {
    var ctx = context.Background();
    resp, err := client.AlterInbound(ctx, &command.AlterInboundRequest{
        Tag: V2rayTag,
        Operation: serial.ToTypedMessage(&command.RemoveUserOperation{
            Email: u.GetEmail(),
        }),
    })
    if err != nil {
        serviceLogger(fmt.Sprintf("Failed to Remove Test Email:%s, Error: %v", u.GetEmail(), err), 31)
    } else {
        resp = resp
    }
}

func GetUserTrafficAndReset(u *VUser) TrafficInfo {
    ti := TrafficInfo{}
    ctx := context.Background()
    up, err := statsClient.GetStats(ctx, &statscmd.GetStatsRequest{
        Name:   fmt.Sprintf(UplinkFormat, u.GetEmail()),
        Reset_: true,
    })
    if err != nil {
        var nilerror = strings.Replace(fmt.Sprintf("%v", err), " ", "", -1)
        var nilerror2 = fmt.Sprintf("user>>>%s>>>traffic>>>uplinknotfound", u.GetEmail())
        if !strings.Contains(nilerror, nilerror2) {
            serviceLogger(fmt.Sprintf("Get User %s's Uplink Traffic Failed, Error: %v", u.GetEmail(), err), 31)
        }
        return ti
    }
    down, err := statsClient.GetStats(ctx, &statscmd.GetStatsRequest{
        Name:   fmt.Sprintf(DownlinkFormat, u.GetEmail()),
        Reset_: true,
    })
    if err != nil {
        var nilerror = strings.Replace(fmt.Sprintf("%v", err), " ", "", -1)
        var nilerror2 = fmt.Sprintf("user>>>%s>>>traffic>>>downlinknotfound", u.GetEmail())
        if !strings.Contains(nilerror, nilerror2) {
            serviceLogger(fmt.Sprintf("Get User %s's Downlink Traffic Failed, Error: %v", u.GetEmail(), err), 31)
        }
        return ti
    }

    ti.Up = up.Stat.Value
    ti.Down = down.Stat.Value

    return ti
}

func getCurrentPath() string {  
    file, _ := exec.LookPath(os.Args[0])  
    path, _ := filepath.Abs(file)  
    path = substr(path, 0, strings.LastIndex(path, "/"))
    return path  
}  

func checkErr(err error) {  
    if err != nil {  
        serviceLogger(fmt.Sprintf("Error: %v!", err), 31)
    }  
}

func substr(s string, pos, length int) string {
    runes := []rune(s)
    l := pos + length
    if l > len(runes) {
        l = len(runes)
    }
    return string(runes[pos:l])
}

func serviceLogger(log string, color int){
    log = strings.Replace(log, "\n", "", -1)
    if(color == 0){
        fmt.Printf("%s\n", log)
    }else{
        fmt.Printf("%c[1;0;%dm%s%c[0m\n", 0x1B, color, log, 0x1B)
    }
}

func timeSleep(second int){
    time.Sleep(time.Duration(second) * time.Second)
}

type VUser struct {
    Email   string `json:"email"`
    UUID    string `json:"uuid"`
    AlterID uint32 `json:"alter_id"`
    Level   uint32 `json:"level"`
    ID      int
}

func (v *VUser) GetEmail() string {
    return v.Email
}

func (v *VUser) GetUUID() string {
    return v.UUID
}

func (v *VUser) GetAlterID() uint32 {
    return v.AlterID
}

func (v *VUser) GetLevel() uint32 {
    return v.Level
}

func (v *VUser) GetID() int {
    return v.ID
}
