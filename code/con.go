package main

import (
        "bytes"
        "encoding/binary"
        "fmt"
        "net"
        "time"
)




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


func main ()  {
      lostpackage := Task_Ping_Probe("172.17.77.0",4)
      fmt.Println(lostpackage)



}
