package Plugins

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/shadow1ng/fscan/common"
	"strings"
	"time"
)

func MysqlScan(info *common.HostInfo) (tmperr error) {
	if common.IsBrute {
		return
	}
	starttime := time.Now().Unix()
	for _, user := range common.Userdict["mysql"] {
		for _, pass := range common.Passwords {
			pass = strings.Replace(pass, "{user}", user, -1)
			flag, err := MysqlConn(info, user, pass)
			if flag == true && err == nil {
				return err
			} else {
				errlog := fmt.Sprintf("[-] mysql %v:%v %v %v %v", info.Host, info.Ports, user, pass, err)
				common.LogError(errlog)
				tmperr = err
				if common.CheckErrs(err) {
					return err
				}
				if time.Now().Unix()-starttime > (int64(len(common.Userdict["mysql"])*len(common.Passwords)) * info.Timeout) {
					return err
				}
			}
		}
	}
	return tmperr
}

func MysqlConn(info *common.HostInfo, user string, pass string) (flag bool, err error) {
	flag = false
	Host, Port, Username, Password := info.Host, info.Ports, user, pass
	dataSourceName := fmt.Sprintf("%v:%v@tcp(%v:%v)/mysql?charset=utf8&timeout=%v", Username, Password, Host, Port, time.Duration(info.Timeout)*time.Second)
	db, err := sql.Open("mysql", dataSourceName)
	if err == nil {
		db.SetConnMaxLifetime(time.Duration(info.Timeout) * time.Second)
		db.SetConnMaxIdleTime(time.Duration(info.Timeout) * time.Second)
		db.SetMaxIdleConns(0)
		defer db.Close()
		err = db.Ping()
		if err == nil {
			result := fmt.Sprintf("[+] mysql:%v:%v:%v %v", Host, Port, Username, Password)
			common.LogSuccess(result)
			res, _ := MysqlSearch(Host, Port, Username, Password)
			common.LogSuccess(res)
			flag = true
		}
	}
	return flag, err
}

func MysqlSearch(Host string, Port string, Username string, Password string) (res string, err error) {
	var db *sql.DB
	sql1 := "select table_name,table_rows,TABLE_SCHEMA from tables order by table_rows desc;"
	dataSourceName := fmt.Sprintf("%v:%v@tcp(%v:%v)/information_schema?charset=utf8", Username, Password, Host, Port)
	db, err = sql.Open("mysql", dataSourceName)
	defer db.Close()
	err = db.Ping()
	if err == nil {
		fmt.Println("[+] 连接成功,开始统计数据")
		rows, err := db.Query(sql1)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var t mysqltable
				err := rows.Scan(&t.table_name, &t.table_rows, &t.TABLE_SCHEMA)
				if err == nil && t.table_rows != 0 {
					re := fmt.Sprintf("表名:%s, 数据量:%d, 位置:%s\n", t.table_name, t.table_rows, t.TABLE_SCHEMA)
					res = res + re
				}
			}
		} else {
			fmt.Println("查询出错")
		}
	}
	return res, err
}

type mysqltable struct {
	table_name   string
	table_rows   int
	TABLE_SCHEMA string
}
