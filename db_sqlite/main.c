#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
//TODO 宏指令定义
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define size_of_attribute(Struct,Attribute) sizeof(((Struct*)0)->Attribute)
#define TABLE_MAX_PAGES 100  //一个表最多占据100页

//TODO 枚举值定义
//元命令解析结果
typedef enum{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
}MetaCommandResult;
//SQL语句解析结果
typedef enum {
    PREPARE_SUCCESS,
    PREPARE_NEGATIVE_ID,
    PREPARE_STRING_TOO_LONG,
    PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_SYNTAX_ERROR
}PrepareResult;
//语句类型
typedef enum{
    STATEMENT_INSERT,
    STATEMENT_SELECT
}StatementType;
//执行结果
typedef enum{
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
}ExecuteResult;
//TODO 数据类型定义
typedef struct {
    char*buffer;
    size_t buffer_length;
    ssize_t input_length;
}InputBuffer;
typedef struct {
    u_int32_t id;
    char username[COLUMN_USERNAME_SIZE+1];
    char email[COLUMN_EMAIL_SIZE+1];
}Row;
typedef struct {
    StatementType type;
    Row row_to_insert;
}Statement;
typedef struct {
    u_int32_t num_rows;
    void * pages[TABLE_MAX_PAGES];
}Table;
//TODO 常量值定义
//表结构常量
const u_int32_t ID_SIZE= size_of_attribute(Row,id);
const u_int32_t USERNAME_SIZE= size_of_attribute(Row,username);
const u_int32_t EMAIL_SIZE= size_of_attribute(Row,email);
const u_int32_t ID_OFFSET=0;
const u_int32_t USERNAME_OFFSET=ID_OFFSET+ID_SIZE;
const u_int32_t EMAIL_OFFSET=USERNAME_OFFSET+USERNAME_SIZE;
const u_int32_t ROW_SIZE=ID_SIZE+USERNAME_SIZE+EMAIL_SIZE;
//页存储常量
const u_int32_t PAGE_SIZE=4096;
const u_int32_t ROWS_PER_PAGE=PAGE_SIZE/ROW_SIZE;
const u_int32_t TABLE_MAX_ROWS=TABLE_MAX_PAGES*ROWS_PER_PAGE;
//TODO 函数声明
InputBuffer * new_input_buffer();
void print_prompt();
void read_input(InputBuffer*inputBuffer);
void close_input_buffer(InputBuffer* InputBuffer);
MetaCommandResult do_meta_command(InputBuffer* inputBuffer);
PrepareResult prepare_insert(InputBuffer* inputBuffer,Statement* statement);
PrepareResult prepare_statement(InputBuffer* inputBuffer,Statement* statement);
ExecuteResult execute_insert(Statement*statement,Table*table);
ExecuteResult execute_select(Statement* statement,Table* table);
ExecuteResult execute_statement(Statement* statement,Table*table);
void serialize_row(Row* source,void*destination);
void deserialize_row(void*source,Row*destination);
void * row_slot(Table* table,u_int32_t row_num);
Table* new_table();
void free_table(Table* table);
void print_row(Row* row);
//TODO 函数定义
InputBuffer * new_input_buffer(){
    InputBuffer *inputBuffer=(InputBuffer*)malloc(sizeof (InputBuffer));
    inputBuffer->buffer=NULL;
    inputBuffer->buffer_length=0;
    inputBuffer->input_length=0;
    return inputBuffer;
}
//打印命令行提示符
void print_prompt(){printf("db> ");}
//输出行数据
void print_row(Row* row){
//    printf("id:%d,username:%s,email:%s\n",row->id,row->username,row->email);
}
//读取数据
void read_input(InputBuffer*inputBuffer){
    ssize_t bytes_read=getline(&(inputBuffer->buffer),&(inputBuffer->buffer_length),stdin);
    if(bytes_read<=0){
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }
    //忽略读取到的尾部的换行符
    inputBuffer->input_length=bytes_read-1;
    inputBuffer->buffer[bytes_read-1]=0;
}
//释放数据流
void close_input_buffer(InputBuffer* InputBuffer){
    free(InputBuffer->buffer);
    free(InputBuffer);
}
//解析命令
MetaCommandResult do_meta_command(InputBuffer* inputBuffer){
    if(strcmp(inputBuffer->buffer,".exit")==0){
        exit(EXIT_SUCCESS);
    }else{
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}
PrepareResult prepare_insert(InputBuffer* inputBuffer,Statement* statement){
    statement->type=STATEMENT_INSERT;
    char* keyword= strtok(inputBuffer->buffer," ");
    char* id_string= strtok(NULL," ");
    char* username= strtok(NULL," ");
    char* email= strtok(NULL," ");

    if(id_string==NULL||username==NULL||email==NULL){
        return PREPARE_SYNTAX_ERROR;
    }
    int id= atoi(id_string);
    if(id<0){
        return PREPARE_NEGATIVE_ID;
    }
    if(strlen(username)>COLUMN_USERNAME_SIZE){
        return PREPARE_STRING_TOO_LONG;
    }
    if(strlen(email)>COLUMN_EMAIL_SIZE){
        return PREPARE_STRING_TOO_LONG;
    }
    statement->row_to_insert.id=id;
    strcpy(statement->row_to_insert.username,username);
    strcpy(statement->row_to_insert.email,email);
    return PREPARE_SUCCESS;
}

// SQL Complier
PrepareResult prepare_statement(InputBuffer* inputBuffer,Statement* statement){
    if(strncmp(inputBuffer->buffer,"insert",6)==0){
        return prepare_insert(inputBuffer,statement);
    }
    if(strncmp(inputBuffer->buffer,"select",6)==0){
        statement->type=STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}
//解析insert
ExecuteResult execute_insert(Statement*statement,Table*table){
    if(table->num_rows>=TABLE_MAX_ROWS){
        return EXECUTE_TABLE_FULL;
    }
    Row* row_to_insert=&(statement->row_to_insert);
    serialize_row(row_to_insert, row_slot(table,table->num_rows));
    table->num_rows+=1;
    return EXECUTE_SUCCESS;
}
ExecuteResult execute_select(Statement* statement,Table* table){
    Row row;
    for(u_int32_t i=0;i<table->num_rows;i++){
        deserialize_row(row_slot(table,i),&row);
        print_row(&row);
    }
    return EXECUTE_SUCCESS;
}
//执行解析结果
ExecuteResult execute_statement(Statement* statement,Table* table){
    switch (statement->type) {
        case STATEMENT_INSERT:
            return execute_insert(statement,table);
        case STATEMENT_SELECT:
            return execute_insert(statement,table);
    }
}
//序列化存储函数
void serialize_row(Row* source,void*destination){
    memcpy(destination+ID_OFFSET,&(source->id),ID_SIZE);
    memcpy(destination+USERNAME_OFFSET,&(source->username),USERNAME_SIZE);
    memcpy(destination+EMAIL_OFFSET,&(source->email),EMAIL_SIZE);
}
//反序列化函数
void deserialize_row(void*source,Row*destination){
    memcpy(&(destination->id),source+ID_OFFSET,ID_SIZE);
    memcpy(&(destination->username),source+USERNAME_OFFSET,USERNAME_SIZE);
    memcpy(&(destination->email),source+EMAIL_OFFSET,EMAIL_SIZE);
}
// 计算row在page中的存储位置
void * row_slot(Table* table,u_int32_t row_num){
    u_int32_t page_num=row_num/ROWS_PER_PAGE;
    void* page=table->pages[page_num];
    if(page==NULL){
        page=table->pages[page_num] = malloc(PAGE_SIZE);
    }
    u_int32_t row_offset=row_num%ROWS_PER_PAGE;
    u_int32_t byte_offset=row_offset*ROW_SIZE;
    return page+byte_offset;
}
//创建新table
Table* new_table(){
    Table* table=(Table*) malloc(sizeof(Table));
    table->num_rows=0;
    for(u_int32_t i=0;i<TABLE_MAX_PAGES;i++){
        table->pages[i]=NULL;
    }
    return table;
}
//释放table
void free_table(Table* table){
    for(u_int32_t i=0;table->pages[i];i++){
        free(table->pages[i]);
    }
    free(table);
}
//TODO main
int main(int argc,char*argv[]) {
    Table* table=new_table();
    InputBuffer* input_buffer=new_input_buffer();
    while(true){
        print_prompt();
        read_input(input_buffer);
        //检查是否是退出之类的元指令
        if(input_buffer->buffer[0] == '.'){
            switch (do_meta_command(input_buffer)) {
                case META_COMMAND_SUCCESS:
                    continue;
                case META_COMMAND_UNRECOGNIZED_COMMAND:
                    printf("Unrecognized command '%s'\n", input_buffer->buffer);
                    continue;
            }
        }
        //指令解析
        Statement statement;
        switch (prepare_statement(input_buffer,&statement)) {
            case PREPARE_SUCCESS:
                break;
            case PREPARE_NEGATIVE_ID:
                printf("ID must be positive.\n");
                continue;
            case PREPARE_STRING_TOO_LONG:
                printf("String is too long.\n");
                continue;
            case PREPARE_UNRECOGNIZED_STATEMENT:
                printf("Unrecognized keyword at start of '%s'.\n",input_buffer->buffer);
                continue;
            case PREPARE_SYNTAX_ERROR:
                printf("Syntax error.Could not parse statement\n");
                continue;
        }
        switch (execute_statement(&statement,table)) {
            case EXECUTE_SUCCESS:
                printf("Executed.\n");
                break;
            case EXECUTE_TABLE_FULL:
                printf("Error:Table full.\n");
                break;
        }
    }
    return 0;
}
