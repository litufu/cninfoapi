package main

import (
	"database/sql"
	"fmt"
	_ "github.com/jackc/pgx/v4/stdlib"
	"log"
	"time"
)

// People - database
type People struct {
	id   int
	name string
	email  string
	created_on  time.Time
	password string
	updated_at time.Time
}

type pgxContext struct {
	db *sql.DB
}


// ConnectDB connect specify database
func connectDB(driverName string, dbName string) (c *pgxContext, errorMessage string) {
	db, err := sql.Open(driverName, dbName)
	if err != nil {
		return nil, err.Error()
	}
	if err = db.Ping(); err != nil {
		return nil, err.Error()
	}
	return &pgxContext{db}, ""
}

// Create
func (c *pgxContext) Create() {
	// get insert id
	lastInsertId := 0
	err := c.db.QueryRow("INSERT INTO users(name,password,email) VALUES($1,$2,$3) RETURNING id", "jack", "123445","jack@126.com").Scan(&lastInsertId)
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println("inserted id is ", lastInsertId)
}

// Read
func (c *pgxContext) Read() {
	rows, err := c.db.Query("SELECT * FROM users")

	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer rows.Close()

	for rows.Next() {
		p := new(People)
		err := rows.Scan(&p.id, &p.name, &p.password,&p.email,&p.created_on,&p.updated_at)
		if err != nil {
			panic(err)
		}
		fmt.Println(p.id, p.name, p.email,p.created_on)
	}

	// check iteration error
	if rows.Err() != nil {
		panic(err)
	}
}

// UPDATE
func (c *pgxContext) Update() {
	stmt, err := c.db.Prepare("UPDATE users SET password = $1 WHERE id = $2")
	if err != nil {
		panic(err)
	}
	result, err := stmt.Exec("121234", 1)
	if err != nil {
		panic(err)
	}
	affectNum, err := result.RowsAffected()
	if err != nil {
		panic(err)
	}
	fmt.Println("update affect rows is ", affectNum)
}

// DELETE
func (c *pgxContext) Delete() {
	stmt, err := c.db.Prepare("DELETE FROM users WHERE name = $1")
	if err != nil {
		log.Fatal(err)
	}
	result, err := stmt.Exec("jack")
	if err != nil {
		log.Fatal(err)
	}
	affectNum, err := result.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("delete affect rows is ", affectNum)
}

//transactions
func (c *pgxContext)Transactions()error{
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare("INSERT INTO users(name,password,email) VALUES($1,$2,$3)")
	_, err = stmt.Exec("andy","fdsa","andy@123.com")
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

//dynamic columns
func (c *pgxContext)Dynamic(){
	rows, err := c.db.Query("SELECT * FROM users")
	if err != nil {
		panic(err)
	}
	cols, err := rows.Columns()
	fmt.Println(cols)
	if err != nil {
		panic(err)
	}

	// 目标列是一个动态生成的数组
	dest := []interface{}{
		new(string),
		new(sql.RawBytes),
		new(sql.RawBytes),
		new(sql.RawBytes),
		new(sql.RawBytes),
		new(sql.RawBytes),
	}

	// 将数组作为可变参数传入Scan中。
	for rows.Next() {
		err := rows.Scan(dest...)
		if err != nil {
			panic(err)
		}
		for _,v := range dest{
			switch v.(type) {
			case *string:
				fmt.Println(*(v.(*string)))
			case *sql.RawBytes:
				fmt.Println(string(*v.(*sql.RawBytes)))
			default:
				fmt.Println("not recognize")
			}
		}
	}

	// check iteration error
	if rows.Err() != nil {
		panic(err)
	}
}

func (c *pgxContext)createDB(){
	_,err := c.db.Exec("CREATE DATABASE pgx_test with owner litufu;")
	if err!=nil{
		fmt.Println(err)
	}
}

func (c *pgxContext)Exec(sql string){
	_,err := c.db.Exec(sql)
	if err!=nil{
		fmt.Println(err.Error(),sql)
	}
}

//createuser and grant
func (c *pgxContext)createUser(){
	_,err := c.db.Exec("create user lily with password '1232456';")
	_,err = c.db.Exec("alter user lily createdb;")
	if err!=nil{
		fmt.Println(err)
	}
}

//func main()  {
//	//c, err := connectDB("postgres", "user=user1 password=password1 dbname=exampledb")
//	//defer c.db.Close()
//	//
//	//if err != "" {
//	//	print(err)
//	//}

//	//c.Create()
//	//c.Read()
//	//c.Update()
//	//c.Delete()
//	//txerr := c.Transactions()
//	//fmt.Println(txerr)
//	//c.Dynamic()
//	//c.createDB()
//	//c.createTable()
//	c.createUser()
//}
