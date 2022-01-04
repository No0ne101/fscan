package Plugins

import (
	"database/sql"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/shadow1ng/fscan/common"
	"strings"
	"time"
)

func MssqlScan(info *common.HostInfo) (tmperr error) {
	if common.IsBrute {
		return
	}
	starttime := time.Now().Unix()
	for _, user := range common.Userdict["mssql"] {
		for _, pass := range common.Passwords {
			pass = strings.Replace(pass, "{user}", user, -1)
			flag, err := MssqlConn(info, user, pass)
			if flag == true && err == nil {
				return err
			} else {
				errlog := fmt.Sprintf("[-] mssql %v:%v %v %v %v", info.Host, info.Ports, user, pass, err)
				common.LogError(errlog)
				tmperr = err
				if common.CheckErrs(err) {
					return err
				}
				if time.Now().Unix()-starttime > (int64(len(common.Userdict["mssql"])*len(common.Passwords)) * info.Timeout) {
					return err
				}
			}
		}
	}
	return tmperr
}

func MssqlConn(info *common.HostInfo, user string, pass string) (flag bool, err error) {
	flag = false
	Host, Port, Username, Password := info.Host, info.Ports, user, pass
	dataSourceName := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%v;encrypt=disable;timeout=%v", Host, Username, Password, Port, time.Duration(info.Timeout)*time.Second)
	db, err := sql.Open("mssql", dataSourceName)
	if err == nil {
		db.SetConnMaxLifetime(time.Duration(info.Timeout) * time.Second)
		db.SetConnMaxIdleTime(time.Duration(info.Timeout) * time.Second)
		db.SetMaxIdleConns(0)
		defer db.Close()
		err = db.Ping()
		if err == nil {
			result := fmt.Sprintf("[+] mssql:%v:%v:%v %v", Host, Port, Username, Password)
			common.LogSuccess(result)
			res, _ := MssqlSearch(Host, Port, Username, Password)
			common.LogSuccess(res)
			flag = true
		}
	}
	return flag, err
}

func MssqlSearch(Host string, Port string, Username string, Password string) (res string, err error) {
	var db *sql.DB
	sql1 := "CREATE TABLE #temp_41c0549e42919922 (TableName VARCHAR (255), RowsCount INT)\nDECLARE @dbname NVARCHAR(500)\nDECLARE @SQL NVARCHAR(4000)\nDECLARE MyCursor CURSOR\nFOR (SELECT Name FROM master..SysDatabases where name not in ('master', 'model', 'msdb', 'tempdb') and status not in (66048, 66056))\nOPEN MyCursor;\nFETCH NEXT FROM MyCursor INTO @dbname;\nWHILE @@FETCH_STATUS = 0\nBegin\nSET @SQL = 'insert into #temp_41c0549e42919922 SELECT ''' + @dbname+'..''+a.name, b.rows FROM '+@dbname+'..sysobjects As a INNER JOIN '+@dbname+'..sysindexes AS b ON a.id = b.id WHERE (a.type = ''u'') AND (b.indid IN (0, 1)) ORDER BY b.rows DESC' \nexec(@SQL);\nFETCH NEXT FROM MyCursor INTO @dbname;\nEnd\nCLOSE MyCursor\nDEALLOCATE MyCursor\nSELECT * FROM #temp_41c0549e42919922;\nDROP TABLE #temp_41c0549e42919922;"
	dataSourceName := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%v;encrypt=disable", Host, Username, Password, Port)
	db, err = sql.Open("mssql", dataSourceName)
	defer db.Close()
	err = db.Ping()
	if err == nil {
		fmt.Println("[+] 连接成功,开始统计数据")
		rows, err := db.Query(sql1)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var t mssqltable
				err := rows.Scan(&t.table_name, &t.table_rows)
				if err == nil && t.table_rows != 0 {
					re := fmt.Sprintf("表名:%s, 数据量:%d\n", t.table_name, t.table_rows)
					res = res + re
				}
			}
		} else {
			fmt.Println("查询出错")
		}
	}
	return res, err
}

type mssqltable struct {
	table_name string
	table_rows int
}
