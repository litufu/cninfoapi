package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const cninfoTokenUrl = "http://webapi.cninfo.com.cn/api-cloud-platform/oauth2/token"

func getToken() string {
	clientId := "0aa70cc4b4314a989c2077d189e6e8da"
	clientSecret := "a67f4e4a6ea44afcb8a8cf11594003de"
	var postData = url.Values{}
	postData.Add("grant_type", "client_credentials")
	postData.Add("client_id", clientId)
	postData.Add("client_secret", clientSecret)

	resp, err := http.PostForm(cninfoTokenUrl, postData)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	var res map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		return ""
	}

	access_token := res["access_token"]
	token := fmt.Sprintf("%s", access_token)
	return token
}

func getParmsString(params map[string]string) string {
	var str []string
	for k, v := range params {
		s := fmt.Sprintf("%s=%s", k, v)
		str = append(str, s)
	}
	res := strings.Join(str, "&")
	return res
}

func httpPost(baseUrl string, params map[string]string) interface{} {
	var postData = url.Values{}
	for k, v := range params {
		postData.Add(k, v)
	}
	resp, err := http.PostForm(baseUrl, postData)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer resp.Body.Close()
	var res map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		log.Fatal(err.Error())
	}
	records := res["records"]
	if (res["resultmsg"] == "success") && (len(records.([]interface{})) >= 1) {
		return records
	}else{
		log.Printf("未取得该接口数据 %s,原因：%s",baseUrl,res["resultmsg"])
		return nil
	}
}

func httpGet(baseUrl string, params map[string]string) interface{} {
	paramString := getParmsString(params)
	url := fmt.Sprintf("%s?%s", baseUrl, paramString)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	defer resp.Body.Close()
	var res map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		return nil
	}
	records := res["records"]
	if (res["resultmsg"] == "success") && (len(records.([]interface{})) >= 1) {
		return records
	}
	return nil
}

//输入值类型
//tdate  Trade Date 交易日期
//bcode Bond Code  债券编码
//platetype 分类代码类型 137001\t市场分类 137002\t证监会行业分类 137003\t巨潮行业分类 137004\t申银万国行业分类 137005\t新财富行业分类 137006\t地区省市分类 137007\t指数成份股 137008\t概念板块
//type 查询类型
//indtype 行业类型 1001 证监会一级行业 1002 证监会二级行业 1003申万行业一级 1004申万行业二级 1005申万行业三级
//orgid 机构ID 来源于基本信息如基金管理公司基本信息
//sdate 开始查询时间 “20190101”
//index 港股指数代码
//edate 结束查询时间 “20200630”
//scode 股票代码
//fcode Fund Code

var configs = []map[string]interface{}{
	{
		"url":           "http://webapi.cninfo.com.cn/api/stock/p_public0004",
		"type":          "once", //执行一次
		"tablename":     "p_public0004",
		"uniqueField":   "SECCODE",
		"initialParams":        []map[string]string{
			{
				"name":"platetype",//参数名称
				"value": "137002",//参数值,列表content参数值使用"a,b,c"
				"type":"single",//参数类型single 一个/list 多个
				"limit":"1",//参数值的个数
				"valueType":"content",//sql 从数据库查询参数值,content：直接传递参数值
			},
		},
		"description":   "返回证监会行业分类信息",
		"checkInterval": "1day",
		"existStrategy": "ignore", //如果存在唯一field是更新还是忽略 update||ignore
		"limit":"",
	},
	{
		"url":           "http://webapi.cninfo.com.cn/api/stock/p_stock2237",
		"type":          "loops", //循环执行
		"tablename":     "p_stock2237",
		"uniqueField":   "SECCODE,F001D",
		"initialParams":        []map[string]string{
			{
				"name":"scode",//参数名称
				"value": "SELECT SECCODE FROM p_public0004",//参数值
				"type":"list",//参数类型single 一个/list 多个
				"limit":"50",//参数值的个数
				"valueType":"sql",//sql 从数据库查询参数值,content：直接传递参数值
			},
		},
		"dailyParams":        []map[string]string{
			{
				"name":"scode",//参数名称
				"value": "SELECT SECCODE FROM p_public0004",//参数值
				"type":"list",//参数类型single 一个/list 多个
				"limit":"50",//参数值的个数
				"valueType":"sql",//sql 从数据库查询参数值,content：直接传递参数值
			},
			{
				"name":"sdate",//参数名称
				"value": yesterday(),//参数值
				"type":"single",//参数类型single 一个/list 多个
				"limit":"1",//参数值的个数
				"valueType":"content",//sql 从数据库查询参数值,content：直接传递参数值
			},
		},
		"description":   "定期报告预披露时间",
		"checkInterval": "1day",
		"existStrategy": "ignore", //如果存在唯一field是更新还是忽略 update||ignore
	},
}

//获取字段类型，根据接口名称和输出字段名称获取字段类型
func getFieldType(interfaceName string, outputFieldName string) (string, error) {
	apiinfo := unmarsalCninfo()
	var output []Output
	err := json.Unmarshal([]byte(apiinfo[interfaceName].OutputParameter), &output)
	if err != nil {
		log.Fatalf("unmarshal output err %s", err)
	}
	for _, v := range output {
		if outputFieldName == v.FieldName {
			return MatchGoType(v.FieldType), nil
		}
	}
	return "", errors.New("无法转换数据类型")
}

//根据sql获取参数列表
func getSqlParams(db *sql.DB,selectSql string)[]string{
	rows, err := db.Query(selectSql)
	if err != nil {
		log.Printf("查询失败%s", err.Error())
	}
	var params []string
	for rows.Next() {
		var row sql.NullString
		err := rows.Scan(&row)
		if err != nil {
			log.Println("")
		}
		if row.Valid {
			params = append(params,row.String)
		}
	}
	return params
}

//检查数据表是否存在某条记录，根据唯一的列检查是否存在
func checkTableExistRecord(db *sql.DB, tableName string, uniqueField string, record map[string]interface{}) bool {
	//唯一字段，可能有两个以上的联合唯一字段a,b
	uniqueFields := splitFields(uniqueField)
	//字段值 用于接收数据库scan
	var fields [] interface{}
	for i:=0;i<len(uniqueFields);i++{
		var a string
		fields = append(fields, &a)
	}
	//获取数据库中的唯一字段值并保存为set
	selectSql := fmt.Sprintf("SELECT %s from %s", uniqueField, tableName)
	rows, err := db.Query(selectSql)
	if err != nil {
		log.Printf("查询失败%s", err.Error())
	}
	set := make(map[string]struct{})
	for rows.Next() {
		err := rows.Scan(fields...)
		if err != nil {
			log.Println("")
		}
		var uniqueString string
		for _,field := range fields{
			if field ==nil{
				continue
			}
			uniqueString += *(field.(*string))
		}
		set[uniqueString] = struct{}{}
	}
	//获取记录中的唯一字段值
	var recordUniqueString string
	for _,uniqueField := range uniqueFields{
		recordUniqueString += record[uniqueField].(string)
	}
	//检查记录中的唯一字段值是否在数据库中
	_, ok := set[recordUniqueString]
	return ok
}

func splitFields(field string)[]string{
	return strings.Split(field,",")
}

//获取参数中的列表参数
func getListParm(params []map[string]string)map[string]string{
	for _,param := range params{
		if param["type"] == "list"{
			return param
		}
	}
	return nil
}

//解析非列表参数
func getNoneListParam(params []map[string]string,token string)map[string]string{
	var parsedParm = make(map[string]string)
	for _,param := range params{
		if param["type"] == "single"{
			parsedParm[param["name"]] = param["value"]
		}
	}
	parsedParm["access_token"] = token
	return parsedParm
}

//将一个列表根据长度分为多个小列表
// s := []string{"a","b","c","c","d","e","f"}
//fmt.Println(splitList(s,2)):[[a b] [c c] [d e] [f]]
func splitList(list []string,limit int)[][]string{
	var res [][]string
	var temp []string
	for i:=0;i<len(list);i+=limit{
		if i+limit > len(list){
			res = append(res, list[i:])
		}else{
			temp = list[i:i+limit]
			res = append(res,temp)
		}
	}
	return res
}

func copyMap(originalMap map[string]string)map[string]string{
	targetMap := make(map[string]string)
	for key, value := range originalMap {
		targetMap[key] = value
	}
	return targetMap
}

//将列表参数和非列表参数组合在一起
func concatParams(noneListParam map[string]string,allParams []string,listParam map[string]string)[]map[string]string{
	var parsedParms []map[string]string
	limit,err := strconv.Atoi(listParam["limit"])
	if err!=nil{
		log.Printf("解析参数limit出错，%s",listParam)
	}
	if (len(allParams) > limit){
		for _,params := range splitList(allParams,limit){
			temp := params
			paramString := strings.Join(temp,",")
			noneListParamCopy := copyMap(noneListParam)
			noneListParamCopy[listParam["name"]] = paramString
			parsedParms = append(parsedParms,noneListParamCopy)
		}
		return parsedParms
	}else{
		noneListParam[listParam["name"]] = listParam["value"]
		parsedParms = append(parsedParms,noneListParam)
		return parsedParms
	}
}

//解析参数配置
func parseParams(params []map[string]string,token string,db *sql.DB)[]map[string]string{
	var parsedParms []map[string]string
	listParam := getListParm(params)
	noneListParam := getNoneListParam(params,token)
	//如果没有列表参数，直接将所有的参数合并
	if listParam == nil{
		parsedParms = append(parsedParms,noneListParam)
		return parsedParms
	}else{
		//根据遍历列表参数生成，将其他非列表参数加入后生成参数列表
		if listParam["valueType"] == "content"{
			//形如"a,b,c,d,e,f"的参数
			allParams := splitFields(listParam["value"])
			parsedParms = concatParams(noneListParam,allParams,listParam)
			return parsedParms
		}else if listParam["valueType"] == "sql"{
			sqlParams := getSqlParams(db,listParam["value"])
			parsedParms = concatParams(noneListParam,sqlParams,listParam)
			return parsedParms
		}else{
			log.Printf("未知列表参数类型,%s",listParam)
			return parsedParms
		}
	}
}

//根据配置表下载所需信息
func downloadInfo() {
	token := getToken()
	c := getContext()
	for _, config := range configs {
		//获取请求参数
		url := config["url"].(string)
		uniqueField := config["uniqueField"].(string) //可能有多个参数构成
		existStrategy := config["existStrategy"].(string) //可能是忽略或者更新
		tablename := config["tablename"].(string) //接口名称和数据表名
		parmas := parseParams(config["initialParams"].([]map[string]string),token,c.db) //可能有多个参数，一个参数可能需要去其他数据库查询
		//请求api,获取请求结果
		for _,param := range parmas{
			records := httpPost(url, param)
			if records == nil {
				return
			}
			//获取keys String
			recordOne := records.([]interface{})[0]
			recordDict := recordOne.(map[string]interface{})
			var keys []string
			for k, _ := range recordDict {
				keys = append(keys, k)
			}
			keysString := strings.Join(keys, ",")
			//获取values
			var values []string
			var deleteRowsField []string
			for _, record := range records.([]interface{}) {
				var allFields []string
				recordDict := record.(map[string]interface{})
				if exist := checkTableExistRecord(c.db, tablename, uniqueField, recordDict); exist {
					if existStrategy == "ignore" {
						continue
					} else if existStrategy == "update" {
						deleteRowsField = append(deleteRowsField, fmt.Sprintf("'%s'", recordDict[uniqueField].(string)))
					} else {
						log.Println("未知存在策略")
					}
				}

				for _, key := range keys {
					v := recordDict[key]
					if v != nil {
						//TODO:如果类型是float或int64处理
						strv := fmt.Sprintf("'%s'", v.(string))
						allFields = append(allFields, strv)
					} else {
						allFields = append(allFields, "NULL")
					}
				}

				valuesString := strings.Join(allFields, ",")
				values = append(values, fmt.Sprintf("(%s)", valuesString))
			}
			//批量删除需要更新的条目
			if len(deleteRowsField) > 0 {
				deleteSql := fmt.Sprintf("DELETE FROM %s WHERE %s in (%s)", tablename, uniqueField, strings.Join(deleteRowsField, ","))
				c.Exec(deleteSql)
			}
			//批量插入
			if len(values)>0{
				insertSql := fmt.Sprintf("INSERT INTO %s(%s) VALUES %s ", tablename, keysString, strings.Join(values, ","))
				c.Exec(insertSql)
			}
		}
	}
}

func main() {
    downloadInfo()
}
