package main

import (
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"
)

//decimal(4,2) numeric (4,2)
//decimal(4) numeric (6,2)
//deciaml  numeric (18,2)
//DECIMAL numeric (18,2)
//DECIMAL(18,2) numeric (18,2)
//numeric(1,0) numeric (18,2)
//NUMBER(14,4) numeric (14,4)
//numeric numeric (18,2)
//NUMBER numeric (18,2)
//number numeric (18,2)
//varchar(400) character varying (400)
//VARCHAR character varying (200)
//varchar character varying (200)
//VARCHAR2(4000) character varying (4000)
//VARCHAR(20) character varying (20)
//varchar60) character varying (60)
//int(8)  bigint
//int bigint
//INT bigint
//bigint bigint
//bigint(20) bigint
//BIGINT(10) bigint
//Date date
//date date
//datetime time  without time zone
//char(1) character(1)
//CHAR(1) character(1)
//char character(5)
//CHAR character(5)
//double  double precision
//text text
//将巨潮资讯apijson中的数据类型转换为对应的postger 数据类型
func MatchPsqlType(originType string)(resultType string){
	//turn string to lowwercase
	str := strings.ToLower(originType)
	charReg := regexp.MustCompile(`.*\((\d+)\)`)
	numberReg := regexp.MustCompile(`.*\((\d+),(\d+)\)`)

	if strings.HasPrefix(str,"decimal") ||
		strings.HasPrefix(str,"numeric") ||
		strings.HasPrefix(str,"number") ||
		strings.HasPrefix(str,"deciaml"){
		res := numberReg.FindStringSubmatch(str)
		if len(res)>1{
			resultType = fmt.Sprintf("numeric(%s,%s)",res[len(res)-2],res[len(res)-1])
		}else{
			resultType = "numeric(18,2)"
		}
	}else if strings.HasPrefix(str,"varchar"){
		res := charReg.FindStringSubmatch(str)
		if len(res)>0{
			resultType = fmt.Sprintf("character varying (%s)",res[len(res)-1])
		}else{
			resultType = "character varying(200)"
		}
	}else if strings.HasPrefix(str,"int"){
		resultType = "bigint"
	}else if strings.HasPrefix(str,"bigint"){
		resultType = "bigint"
	}else if strings.HasPrefix(str,"datetime"){
		resultType = "character varying(25)"
	}else if strings.HasPrefix(str,"date"){
		resultType = "character varying(25)"
	}else if strings.HasPrefix(str,"char"){
		res := charReg.FindStringSubmatch(str)
		if len(res)>0{
			resultType = fmt.Sprintf("character(%s)",res[len(res)-1])
		}else{
			resultType = "character(10)"
		}
	}else if strings.HasPrefix(str,"double"){
		resultType = "double precision"
	}else if strings.HasPrefix(str,"text"){
		resultType = "text"
	}else{
		log.Fatalf("未找到apiinfo.json中的数据类型,%s",str)
	}
	return
}

func strToTime(ts string)(time.Time,error){
	var timeLayoutStr = "2006-01-02 15:04:05"
	return time.Parse(timeLayoutStr, ts)
}

func strToDate(ts string)(time.Time,error){
	var timeLayoutStr = "2006-01-02"
	return time.Parse(timeLayoutStr, ts)
}

func yesterday()string{
	nTime := time.Now()
	yesTime := nTime.AddDate(0,0,-1)
	yesterdayString := yesTime.Format("20060102")
	return yesterdayString
}


//将巨潮资讯apijson中的数据类型转换为对应的go数据类型
func MatchGoType(originType string)(resultType string){
	//turn string to lowwercase
	str := strings.ToLower(originType)
	if strings.HasPrefix(str,"decimal") ||
		strings.HasPrefix(str,"numeric") ||
		strings.HasPrefix(str,"number") ||
		strings.HasPrefix(str,"deciaml")||
		strings.HasPrefix(str,"double"){
		resultType = "float64"
	}else if strings.HasPrefix(str,"varchar") ||strings.HasPrefix(str,"char") ||strings.HasPrefix(str,"text"){
		resultType = "string"
	}else if strings.HasPrefix(str,"int") || strings.HasPrefix(str,"bigint"){
		resultType = "int64"
	}else if strings.HasPrefix(str,"datetime"){
		resultType = "string"
	}else if strings.HasPrefix(str,"date"){
		resultType = "string"
	}else{
		log.Fatalf("未找到apiinfo.json中的数据类型,%s",str)
	}
	return
}

