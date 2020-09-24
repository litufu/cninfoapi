package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
)

type ApiInfo struct {
	Alias  string `json:"alias"`
	ApiId  int `json:"apiId"`
	CacheUpdateMode  int `json:"cacheUpdateMode"`
	CacheUpdateTime  int `json:"cacheUpdateTime"`
	Command  string `json:"command"`
	CreateUser  string `json:"createUser"`
	DataType  int `json:"dataType"`
	Del  int `json:"del"`
	FullUrl  string `json:"fullUrl"`
	FunRelStatus  int `json:"funRelStatus"`
	Id  int `json:"id"`
	InputParameter  string `json:"inputParameter"`
	LimitParam  int `json:"limitParam"`
	MaxRecordsParam  int `json:"maxRecordsParam"`
	Name  string `json:"name"`
	OrderbyParam  int `json:"orderbyParam"`
	OutputDescribes  string `json:"outputDescribes"`
	OutputParameter  string `json:"outputParameter"`
	PrefixName  string `json:"prefixName"`
	Status  int `json:"status"`
	TbRelStatus  int `json:"tbRelStatus"`
	Type  int `json:"type"`
	UpdateUser  string `json:"updateUser"`
	Url  string `json:"type"`
}

type Input struct {
	FieldType  string `json:"fieldType"`
	FieldChineseName  string `json:"fieldChineseName"`
	IsNeed  string `json:"isNeed"`
	Describe  string `json:"describe"`
	FieldName  string `json:"fieldName"`
	DefaultValue  string `json:"defaultValue"`
	CheckRule  string `json:"checkRule"`
}

type Output struct {
	FieldName  string `json:"fieldName"`
	FieldChineseName  string `json:"fieldChineseName"`
	FieldType  string `json:"fieldType"`
	Describe  string `json:"describe"`
}

func genrateSql(tablename string){
	sql := `CREATE TABLE %s (
    code        char(5) CONSTRAINT firstkey PRIMARY KEY,
    title       varchar(40) NOT NULL,
    did         integer NOT NULL,
    date_prod   date,
    kind        varchar(10),
    len         interval hour to minute
);`
fmt.Println(sql)
}

func getContext()*pgxContext{
	c, errinfo := connectDB("pgx", "postgres://postgres:123456abc@localhost:5432/goauth?sslmode=disable")
	if errinfo != "" {
		fmt.Println(errinfo)
	}
	return c
}

//从apiinfo.json中解析数据到map[string]ApiInfo
func unmarsalCninfo()map[string]ApiInfo{


	content,err := ioutil.ReadFile("./cninfo/apiinfo.json")
	if err!=nil{
		log.Fatal("can't read apiinfo.json %s",err.Error())
	}
	var apiinfo map[string]ApiInfo
	err =json.Unmarshal(content,&apiinfo)
	if err!=nil{
		log.Fatal("can't unmarshal apiinfo,%s",err.Error())
	}
	return apiinfo
}

//根据apiinfo.json的outputparm 生成数据库表
func createTable(){
	apiinfo := unmarsalCninfo()
	c := getContext()
	for key,value := range apiinfo{
		var output []Output
		err := json.Unmarshal([]byte(value.OutputParameter),&output)
		if err != nil{
			log.Fatalf("unmarshal output err %s",err)
		}
		var items []string
		for _,v := range output{
			columnName := v.FieldName
			columnType := MatchPsqlType(v.FieldType)
			item := fmt.Sprintf("%s %s",columnName,columnType)
			items = append(items, item)
		}
		sql := fmt.Sprintf("CREATE TABLE %s (%s);",key,strings.Join(items,","))

		c.Exec(sql)
	}
}

//删除根据apiinfo.json output para 生成的数据库表
func dropTable(){
	apiinfo := unmarsalCninfo()
	c := getContext()
	for key,_ := range apiinfo{
		sql := fmt.Sprintf("DROP TABLE %s ;",key)
		c.Exec(sql)
	}
}


//
//func main()  {
//	apiinfo := unmarsalCninfo()
//	var names = make(map[string]string)
//
//	for _,value := range apiinfo {
//		var input []Input
//		err := json.Unmarshal([]byte(value.InputParameter), &input)
//		if err != nil {
//			log.Fatalf("unmarshal output err %s", err)
//		}
//		for _,v := range input{
//			if v.IsNeed == "1"{
//				names[v.FieldName]=v.FieldChineseName
//			}
//		}
//	}
//	for k,v := range names{
//		fmt.Println(k,v)
//	}
//}
