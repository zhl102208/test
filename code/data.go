package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	_ "github.com/go-sql-driver/mysql"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

var db *sql.DB

func init() {
	db, _ = sql.Open("mysql", "zabbix:zabbix@(172.16.19.11:3306)/zabbix?charset=utf8")
	db.Ping()
}

type Response struct {
	Gpu_temp        []string `json:"gpu_temp"`
	Cpu_temp        []string `json:"cpu_temp"`
	Docker_number   string   `json:"docker_number"`
	Docker_status   string   `json:"docker_status"`
	Net_status      string   `json:"net_status"`
	Mem_utilization string   `json:"mem_utilization"`
	Gpu_util        []string `json:"gpu_util"`
	Board_temp      string   `json:"board_temp"`
	Disk_temp       []string `json:"disk_temp"`
	Disk_smart      string   `json:"disk_smart"`
	Disk_io         []string `json:"disk_io"`
	Disk_util       []string `json:"disk_util"`
        Cpu_util       []string `json:"cpu_util"`
}

func GetHostList() (hostlist string) {
	var host_string string
	rows, err := db.Query("select   host FROM   hosts where   host like '172.16%'")
	if err == nil {
		var host []byte
		for rows.Next() {
			err := rows.Scan(&host)
			if err == nil {
				//              fmt.Println(string(host))
				host_string += "," + string(host)
			}
		}
	}
	if len(host_string) > 3 {
		host_string = host_string[1:]
	}
	return host_string
}
func GetVpctList(hostip string) (hostlist string) {
	var host_string string
	rows, err := db.Query("select   value  from   history_text  where itemid in (select itemid from items where  hostid IN (SELECT  hostid  FROM   hosts WHERE host like '" + hostip + "%') and name ='docker.mqqueues' ) and   clock>UNIX_TIMESTAMP(SUBDATE(now(),interval 100 second)) limit 1")
	if err == nil {
		var value []byte
		for rows.Next() {
			err := rows.Scan(&value)
			if err == nil {
				host_string += "," + string(value)
			}
		}
	}
	if len(host_string) > 3 {
		host_string = strings.Replace(host_string, "\n", ",", -1)
		host_string = host_string[1:]
	}
	return host_string
}

type AutoGenerated []struct {
	MessageStats struct {
		DeliverGetDetails struct {
			Rate float64 `json:"rate"`
		} `json:"deliver_get_details"`
		MessagesReadyDetails struct {
			Rate float64 `json:"rate"`
		} `json:"messages_ready_details"`

		PublishDetails struct {
			Rate float64 `json:"rate"`
		} `json:"publish_details"`
	} `json:"message_stats,omitempty"`
	State    string `json:"state"`
	Name     string `json:"name"`
	Vhost    string `json:"vhost"`
	Messages int    `json:"messages"`
}

func ShowMqQueus(ip, vhost string, dbnumber int) (quen_list string) {
	var s1 string
	var p AutoGenerated
	s := system("curl --silent -u  mgtv:mgtv123 http://" + ip + ":15672/api/queues/" + vhost)
	err := json.Unmarshal([]byte(s), &p)
	if err == nil {
		for _, v := range p {
			s1 += "\"" + v.Name + "\":\"" + strconv.Itoa(v.Messages) + "\","
		}
	}
	s1 = "{" + s1[:len(s1)-1] + "}"
	AddString(dbnumber, "online_queue_list", s1)
	return s1

}
func system(s string) (v string) {
	cmd := exec.Command("/bin/sh", "-c", s)
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println(err)
	}
	return string(out)
}

func UpdateRedisList(data_type, json_str string) { //物理服务器列表
	DelList(1, data_type) //删除数据
	array_line := strings.Split(json_str, ",")
	for i := 0; i < len(array_line); i++ {
                if len(array_line[i]) > 8 {
		AddList(1, data_type, array_line[i])
                }
	}
}

func AddList(dbnumber int, listname, listitem string) {
	options := redis.DialDatabase(dbnumber)
	c, err := redis.Dial("tcp", "127.0.0.1:6379", options)
	if err != nil {
		fmt.Println("conn to redis error", err.Error())
		return
	}
	defer c.Close()
	_, err = c.Do("lpush", listname, listitem)
	if err != nil {
		fmt.Println("redis lpush failed", err.Error())
	}
}

func DelList(dbnumber int, listname string) {
	options := redis.DialDatabase(dbnumber)
	c, err := redis.Dial("tcp", "127.0.0.1:6379", options)
	if err != nil {
		fmt.Println("conn to redis error", err.Error())
		return
	}

	defer c.Close()
	num, err := c.Do("llen", listname)
	if err != nil {
		fmt.Println("mylist get len err", err.Error())
	} else {
		fmt.Println("mylist's len is ", num)
	}
	n1 := fmt.Sprintf("%d", num)
	n2, err := strconv.Atoi(n1)
	for i := 0; i < n2; i++ {
		_, err = c.Do("lpop", listname)
		if err != nil {
			fmt.Println("lpop failed:", err.Error())
		} else {

		}
	}
}

func AddString(dbnumber int, strname, stritem string) {
	options := redis.DialDatabase(dbnumber)
	c, err := redis.Dial("tcp", "127.0.0.1:6379", options)
	if err != nil {
		fmt.Println("conn to redis error", err.Error())
		return
	}
	defer c.Close()
	_, err = c.Do("set", strname, stritem)
	if err != nil {
		fmt.Println("redis lpush failed", err.Error())
	}
}

func AddHash(dbnumber int, hashname, hashkey, hashvalue string) {
	options := redis.DialDatabase(dbnumber)
	c, err := redis.Dial("tcp", "127.0.0.1:6379", options)
	if err != nil {
		fmt.Println("conn to redis error", err.Error())
		return
	}
	defer c.Close()
	_, err = c.Do("hset", hashname, hashkey, hashvalue)
	if err != nil {
		fmt.Println("haset failed", err.Error())
	}
}

func DelHash(dbnumber int, hashname, hashkey string) {
	options := redis.DialDatabase(dbnumber)
	c, err := redis.Dial("tcp", "127.0.0.1:6379", options)
	if err != nil {
		fmt.Println("conn to redis error", err.Error())
		return
	}
	defer c.Close()
	_, err = c.Do("hdel", hashname, hashkey)
	if err != nil {
		fmt.Println("haset failed", err.Error())
	}
}
func GetListNumber(dbnumber int, hashname string)(n1 string) {
        options := redis.DialDatabase(dbnumber)
        c, err := redis.Dial("tcp", "127.0.0.1:6379", options)
        if err != nil {
                fmt.Println("conn to redis error", err.Error())
                return
        }
        defer c.Close()
        n, err := c.Do("llen", hashname)
        if err != nil {
                fmt.Println("haset failed", err.Error())
        }
        n1 = fmt.Sprintf("%d",n)
         return  n1
}




//========================================新的函数=================

func GetGpuTemp(ipstring string) (valuestring string) { // 返回GPU的温度
	var host_string string
	var value []byte
	rows, err := db.Query("select value from history_uint,  (select  itemid as ait,max(clock) as av  from  history_uint   where itemid  in (select itemid from items where name  like  '%gpu%' and  name like '%temp%' and  hostid in  (SELECT  hostid  FROM   hosts WHERE host like '" + ipstring + "%')) and  clock >UNIX_TIMESTAMP(SUBDATE(now(),interval 3000 second)) group by itemid) a where  history_uint.itemid=a.ait and history_uint.clock=a.av")
	fmt.Println(err)
	if err == nil {
		for rows.Next() {
			err := rows.Scan(&value)
			if err == nil {
				host_string += string(value) + ","
                                fmt.Println("GPUTEMP",string(value))
			}
		}
	}
	if len(host_string) > 3 {
		host_string = host_string[:len(host_string)-1]
	}
	return host_string
}

func GetCpuTemp(ipstring string) (valuestring string) { // 返回GPU的温度
        var host_string string
        var value []byte
        rows, err := db.Query("select value from history_uint,  (select  itemid as ait,max(clock) as av  from  history_uint   where itemid  in (select itemid from items where name  like  '%cpu%' and  name like '%temp%' and  hostid in  (SELECT  hostid  FROM   hosts WHERE host like '" + ipstring + "%')) and  clock >UNIX_TIMESTAMP(SUBDATE(now(),interval 3000 second)) group by itemid) a where  history_uint.itemid=a.ait and history_uint.clock=a.av")
        fmt.Println(err)
        if err == nil {
                for rows.Next() {
                        err := rows.Scan(&value)
                        if err == nil {
                                host_string += string(value) + ","
                        }
                }
        }
        if len(host_string) > 3 {
                host_string = host_string[:len(host_string)-1]
        }
        return host_string
}

func GetFsUtil(ipstring string)(valuestring string){ // 返回文件系统的使用率
    var  host_string  string
    var value [] byte 
    rows,err := db.Query("select value  from  history where itemid in (select itemid  from  items,hosts where hosts.hostid=items.hostid  and host ='"+ipstring+"' and items.name in ('fz.size')) order by  clock desc   limit 1 ;")
    if err  == nil{
       for rows.Next() {
            err := rows.Scan(&value)
            if err == nil {
               host_string=string(value)
            }
        }
     }
  return   host_string 
}


func GetGpuUtil(ipstring string)(valuestring string){ // 返回GPU的使用率
    var  host_string  string
    var value [] byte
    rows,err := db.Query("select value from history_uint,  (select  itemid as ait,max(clock) as av  from  history_uint   where itemid  in (select itemid from items where name  like 'gpu%' and  name like '%.util' and name  not like '%memory%' and  hostid in  (SELECT  hostid  FROM   hosts WHERE host like '" + ipstring + "%')) and  clock >UNIX_TIMESTAMP(SUBDATE(now(),interval 3000 second)) group by itemid) a where  history_uint.itemid=a.ait and history_uint.clock=a.av")
    if err  == nil{
       for rows.Next() {
            err := rows.Scan(&value)
            if err == nil {
               host_string+=string(value)+","
            }
       }
    }
if len(host_string) > 3 {
                host_string = host_string[:len(host_string)-1]
        }
  return   host_string   
}



func GetMemUtil(ipstring string) (valuestring string) { // 返回内存使用kv
        var host_string string
        var value []byte
        rows, err := db.Query("select value  from  history_uint where    itemid  in (select itemid from  items where name='Total memory' and hostid in  (SELECT hostid FROM hosts WHERE  host='" + ipstring + "'))   limit 1 union  select value  from   history_uint where  clock >UNIX_TIMESTAMP(SUBDATE(now(),interval 3000 second)) and   itemid  in (select itemid from  items where name='Available memory' and hostid in  (SELECT hostid FROM hosts WHERE  host='" + ipstring + "'))   limit 2")
        if err == nil {
                for rows.Next() {
                        err := rows.Scan(&value)
                        if err == nil {
                                host_string += string(value) + ","
                        }
                }
        }
if len(host_string) > 3 {
                host_string = host_string[:len(host_string)-1]
        }
  return   host_string
        return host_string
}

// 获取CPU的一分钟的利用率
func GetCPUUtil(ipstring string)(valuestring string){
    var  host_string  string
    var value [] byte
    rows,err := db.Query("select value  from  history where itemid in (select itemid  from  items,hosts where hosts.hostid=items.hostid  and host ='"+ipstring+"' and items.name in ('Processor load (1 min average per core)')) order by  clock desc   limit 1 ;")
    if err  == nil{
       for rows.Next() {
            err := rows.Scan(&value)
            if err == nil {
               host_string=string(value)
            }
        }
     }
  return   host_string
}


type ICMP struct {
        Type        uint8
        Code        uint8
        CheckSum    uint16
        Identifier  uint16
        SequenceNum uint16
}

func getICMP(seq uint16) ICMP {
        icmp := ICMP{
                Type:        8,
                Code:        0,
                CheckSum:    0,
                Identifier:  0,
                SequenceNum: seq,
        }

        var buffer bytes.Buffer
        binary.Write(&buffer, binary.BigEndian, icmp)
        icmp.CheckSum = CheckSum(buffer.Bytes())
        buffer.Reset()

        return icmp
}

func sendICMPRequest(icmp ICMP, destAddr *net.IPAddr) error {
        conn, err := net.DialIP("ip4:icmp", nil, destAddr)
        if err != nil {
                fmt.Printf("Fail to connect to remote host: %s\n", err)
                return err
        }
        defer conn.Close()

        var buffer bytes.Buffer
        binary.Write(&buffer, binary.BigEndian, icmp)

        if _, err := conn.Write(buffer.Bytes()); err != nil {
                return err
        }
        tStart := time.Now()
        conn.SetReadDeadline((time.Now().Add(time.Second * 2)))
        recv := make([]byte, 1024)
        receiveCnt, err := conn.Read(recv)
        if err != nil {
                return err
        }

        tEnd := time.Now()
        duration := tEnd.Sub(tStart).Nanoseconds() / 1e6
        fmt.Printf("%d bytes from %s: seq=%d time=%dms\n", receiveCnt, destAddr.String(), icmp.SequenceNum, duration)
        return err
}

func CheckSum(data []byte) uint16 {
        var (
                sum    uint32
                length int = len(data)
                index  int
        )
        for length > 1 {
                sum += uint32(data[index])<<8 + uint32(data[index+1])
                index += 2
                length -= 2
        }
        if length > 0 {
                sum += uint32(data[index])
        }
        sum += (sum >> 16)

        return uint16(^sum)
}

func Task_Ping_Probe(host string, package_number int) (lostnumber int) {
        var lost_umber,res_number int
        raddr, err := net.ResolveIPAddr("ip", host)
        if err != nil {
                fmt.Printf("Fail to resolve %s, %s\n", host, err)
                //return
        }

        fmt.Printf("Ping %s (%s):\n\n", raddr.String(), host)

        for i := 0; i < package_number; i++ {
                if err = sendICMPRequest(getICMP(uint16(i)), raddr); err != nil {
                        fmt.Printf("Error: %s\n", err)
                        lost_umber++
                }
                time.Sleep(1 * time.Second)
        }

        fmt.Println(time.Now().Format("2006-01-02 15:04:05"), host, package_number, package_number-lost_umber)
        res_number = package_number-lost_umber     
        return  res_number

}


func main() {
	HostString := GetHostList()
	fmt.Println(time.Now().Format("2006-01-02 15:04:05"), "获取主机的IP列表如下", HostString)
	UpdateRedisList("online_server_list", HostString)
	var vpcstr string
	array_line := strings.Split(HostString, ",")
	for i := 0; i < len(array_line); i++ {
                if   len(array_line[i]) > 5 {
		vpcstr += GetVpctList(array_line[i]) + ","
                }
	}

	fmt.Println(time.Now().Format("2006-01-02 15:04:05"), "获取主机的VPC列表如下", vpcstr[:len(vpcstr)-1])
	UpdateRedisList("online_vpc_list", vpcstr[:len(vpcstr)-1])
	quenlist := ShowMqQueus("172.16.19.2", "ai", 1)
	fmt.Println(time.Now().Format("2006-01-02 15:04:05"), "获取MQ的QUEMU列表如下", quenlist)

	array_line_host := strings.Split(HostString, ",")
	for i := 0; i < len(array_line_host); i++ { // 开始展示需要的IP列表的数据
		var P Response
		var str string
                var  cpunumber int

		// 开始完成GPU温度的采集
		str = GetGpuTemp(array_line_host[i])
		fmt.Println("++++++++++GPU_TEMP_list开始获取GPU的温度的列表", str)
                ArrayLineGetGpuTemp := strings.Split(str,",")
                if len(ArrayLineGetGpuTemp) > 0 {
                   for i := 0; i < len(ArrayLineGetGpuTemp) ; i ++ {
                       P.Gpu_temp =append(P.Gpu_temp,ArrayLineGetGpuTemp[i])

                   }
                }
                
                // 开始完成CPU温度的采集
                str = GetCpuTemp(array_line_host[i])
                fmt.Println("++++++++++CPU_TEMP_list开始获取CPU的温度的列表", str)
                ArrayLineGetCpuTemp := strings.Split(str,",")
                cpunumber = len(ArrayLineGetCpuTemp)    
                if len(ArrayLineGetCpuTemp) > 0 {
                   for i := 0; i < len(ArrayLineGetCpuTemp) ; i ++ {
                       P.Cpu_temp =append(P.Cpu_temp,ArrayLineGetCpuTemp[i])

                   }
                }
                    
                //开始完成文件系统（磁盘的利用率）
                  str = GetFsUtil(array_line_host[i])
                  fmt.Println("++++++++++C开始获取磁盘的利用率", str)
                  P.Disk_util =append(P.Disk_util,str)
                
                //开始完成GPU利用率的采集
                 str =  GetGpuUtil(array_line_host[i])
                 fmt.Println("++++++++++GUP 的利用率", str) 
                  ArrayLineGetGpuUtil := strings.Split(str,",")
                if len(ArrayLineGetGpuUtil) > 0 {
                   for i := 0; i < len(ArrayLineGetGpuUtil) ; i ++ {
                       f1,_ := strconv.ParseFloat(ArrayLineGetGpuUtil[i],32/64)
                       f3 := f1/100
                       str2 := fmt.Sprintf("%f", f3)
                       P.Gpu_util =append(P.Gpu_util,str2)

                   }
                }


                // 开始获取内存使用率
                str =  GetMemUtil(array_line_host[i])
                fmt.Println("++++++++++Mem 的利用率", str)
                ArrayLineGetMemUtil := strings.Split(str,",")
                if len(ArrayLineGetMemUtil) ==2 {
                   fmt.Println(ArrayLineGetMemUtil[0],ArrayLineGetMemUtil[1])
                   f1,_ := strconv.ParseFloat(ArrayLineGetMemUtil[0],32/64) 
                   f2,_ := strconv.ParseFloat(ArrayLineGetMemUtil[1],32/64)  
                   f3 := f2/f1
                   str2 := fmt.Sprintf("%f", f3)   
                   P.Mem_utilization =str2  
                }
                

                //开始获取CPU的利用率
                str =  GetCPUUtil(array_line_host[i])
                
                fmt.Println("++++++++++CPU 的利用率", str) 
                f1,_ := strconv.ParseFloat(str,32/64) 
                f2,_ := strconv.ParseFloat(strconv.Itoa(cpunumber),32/64)
                f3 := f1/f2
                s1 :=fmt.Sprintf("%f", f3)
                for i := 0; i < cpunumber; i ++ {
                    P.Cpu_util =append(P.Cpu_util,s1)
                }

                
                // 开始获取Vpc列表
                 str =  GetVpctList(array_line_host[i])
                 fmt.Println("++++++++++VPC列表", str)
                 ArrayLineGetVpctList := strings.Split(str,",")
                 if len(ArrayLineGetVpctList) > 0 {
                 P.Docker_number=strconv.Itoa(len(ArrayLineGetVpctList))
                 P.Docker_status ="running"
                 P.Net_status = "connect"
                 } else {
                 P.Docker_number="0"
                 P.Docker_status ="stop"
                 P.Net_status = "disconnect"

                 }
                 P.Disk_temp=append(P.Disk_temp,"28")
                 P.Board_temp = "35"
                 P.Disk_smart  ="disk is ok"
                 P.Disk_io =append(P.Disk_io,"12")              

		bytes, _ := json.Marshal(P)
		fmt.Println(string(bytes))
		DelHash(1, "online_server_info", array_line_host[i])
		AddHash(1, "online_server_info", array_line_host[i], string(bytes))
	}

}
