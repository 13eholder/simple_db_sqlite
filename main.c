#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
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
    int file_descriptor;
    u_int32_t file_length;
    void* pages[TABLE_MAX_PAGES];
}Pager;
typedef struct {
    u_int32_t num_rows;
//    void * pages[TABLE_MAX_PAGES];
    Pager* pager;
}Table;
typedef struct{
    Table* table;
    u_int32_t row_num;
    bool end_of_table;
}Cursor;

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
MetaCommandResult do_meta_command(InputBuffer* inputBuffer,Table* table);
PrepareResult prepare_insert(InputBuffer* inputBuffer,Statement* statement);
PrepareResult prepare_statement(InputBuffer* inputBuffer,Statement* statement);
ExecuteResult execute_insert(Statement*statement,Table*table);
ExecuteResult execute_select(Statement* statement,Table* table);
ExecuteResult execute_statement(Statement* statement,Table*table);
void serialize_row(Row* source,void*destination);
void deserialize_row(void*source,Row*destination);
Table* db_open(const char* filename);
void pager_flush(Pager*pager,u_int32_t page_num,u_int32_t size);
void db_close(Table* table);
Pager* pager_open(const char* filename);
void* get_page(Pager* pager,u_int32_t page_num);
void print_row(Row* row);
Cursor* table_start(Table* table);
Cursor* table_end(Table* table);
void* cursor_value(Cursor* cursor);
void cursor_advance(Cursor* cursor);
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
   printf("id:%d,username:%s,email:%s\n",row->id,row->username,row->email);
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
MetaCommandResult do_meta_command(InputBuffer* inputBuffer,Table* table){
    if(strcmp(inputBuffer->buffer,".exit")==0){
        db_close(table);
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
    Cursor* cursor=table_end(table);
    serialize_row(row_to_insert, cursor_value(cursor));
    table->num_rows+=1;
    free(cursor);
    return EXECUTE_SUCCESS;
}
ExecuteResult execute_select(Statement* statement,Table* table){
    statement->type=STATEMENT_SELECT;
    Cursor* cursor=table_start(table);
    Row row;
    while(!cursor->end_of_table){
        deserialize_row(cursor_value(cursor), &row );
        print_row(&row);
        cursor_advance(cursor);
    }
    free(cursor);
    return EXECUTE_SUCCESS;
}
//执行解析结果
ExecuteResult execute_statement(Statement* statement,Table* table){
    switch (statement->type) {
        case STATEMENT_INSERT:
            return execute_insert(statement,table);
        case STATEMENT_SELECT:
            return execute_select(statement,table);
    }
}
//序列化存储函数
void serialize_row(Row* source,void*destination){
    memcpy(destination+ID_OFFSET,&(source->id),ID_SIZE);
    strncpy(destination+USERNAME_OFFSET,source->username,USERNAME_SIZE);
    strncpy(destination+EMAIL_OFFSET,source->email,EMAIL_SIZE);
}
//反序列化函数
void deserialize_row(void*source,Row*destination){
    memcpy(&(destination->id),source+ID_OFFSET,ID_SIZE);
    memcpy(&(destination->username),source+USERNAME_OFFSET,USERNAME_SIZE);
    memcpy(&(destination->email),source+EMAIL_OFFSET,EMAIL_SIZE);
}
//创建一个Cursor,指向表的开头
Cursor* table_start(Table* table){
    Cursor* cursor=malloc(sizeof(Cursor));
    cursor->table=table;
    cursor->row_num=0;
    cursor->end_of_table=(table->num_rows==0);
    return cursor;
}
//创建一个Cursor,指向表的结尾
Cursor* table_end(Table* table){
    Cursor* cursor =malloc(sizeof(Cursor));
    cursor->table=table;
    cursor->row_num=table->num_rows;
    cursor->end_of_table=true;
    return cursor;
}
//返回一个 指向当前游标指向行的 指针
void* cursor_value(Cursor* cursor){
    u_int32_t row_num=cursor->row_num;
    u_int32_t page_num=row_num/ROWS_PER_PAGE;
    void* page= get_page(cursor->table->pager,page_num);
    u_int32_t row_offset=row_num%ROWS_PER_PAGE;
    u_int32_t byte_offset=row_offset*ROW_SIZE;
    return page+byte_offset;
}
//使游标指向下一行
void cursor_advance(Cursor* cursor){
    if(!cursor->end_of_table){
        cursor->row_num+=1;
        cursor->end_of_table=cursor->row_num>=cursor->table->num_rows;
    }
}
//从文件中读取储存的数据库信息
Table* db_open(const char* filename){
    Pager* pager=pager_open(filename);
    u_int32_t num_rows=pager->file_length/ROW_SIZE;
    Table* table= malloc(sizeof(Table));
    table->pager=pager;
    table->num_rows=num_rows;
    return table;
}
//关闭数据库，写回数据，释放内存
void db_close(Table* table){
    Pager* pager=table->pager;
    u_int32_t num_full_pages=table->num_rows/ROWS_PER_PAGE;
    //写回数据
    for(u_int32_t i=0;i<num_full_pages;i++){
        if(pager->pages[i]==NULL){
            continue;
        }
        pager_flush(pager,i,PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i]=NULL;
    }
    u_int32_t num_additional_row=table->num_rows%ROWS_PER_PAGE;
    if(num_additional_row>0){
        if(pager->pages[num_full_pages]!=NULL){
            pager_flush(pager,num_full_pages,num_additional_row*ROW_SIZE);
            free(pager->pages[num_full_pages]);
            pager->pages[num_full_pages]=NULL;
        }
    }
    //关闭文件
    int result= close(pager->file_descriptor);
    if(result==-1){
        printf("Error close file.\n");
        exit(EXIT_FAILURE);
    }
    //释放空缓存的空间
    for(u_int32_t i=0;i<TABLE_MAX_PAGES;i++){
        void* page=pager->pages[i];
        if(page){
            free(page);
            pager->pages[i]=NULL;
        }
    }
    free(pager);
    free(table);
}
//将某一页的内容写入内存
void pager_flush(Pager*pager,u_int32_t page_num,u_int32_t size){
    if(pager->pages[page_num]==NULL){
        printf("Tried to flush null page.\n");
        exit(EXIT_FAILURE);
    }
    off_t offset= lseek(pager->file_descriptor,page_num*PAGE_SIZE,SEEK_SET);
    if(offset==-1){
        printf("Error seeking:%d\n",errno);
        exit(EXIT_FAILURE);
    }
    ssize_t bytes_written= write(pager->file_descriptor,pager->pages[page_num],size);
    if(bytes_written==-1){
        printf("Error writing:%d\n",errno);
        exit(EXIT_FAILURE);
    }

}
//从文件中读取数据，初始化pager
Pager* pager_open(const char* filename){
    // O_RDWR Read/Write mode
    // O_CREAT Create file if it does not exist
    // S_IWUSR User write permission
    // S_IRUSR User read permission
    int fd= open(filename,O_RDWR|O_CREAT,S_IWUSR|S_IRUSR);
    if(fd==-1){
        printf("Unable to open file.\n");
        exit(EXIT_FAILURE);
    }
    off_t file_length= lseek(fd,0,SEEK_END);
    Pager* pager= malloc(sizeof(Pager));
    pager->file_descriptor=fd;
    pager->file_length=file_length;
    for(u_int32_t i=0;i<TABLE_MAX_PAGES;i++){
        pager->pages[i]=NULL;
    }
    return pager;
}
//从pager中获取page_num对应的page
void* get_page(Pager* pager,u_int32_t page_num){
    //Cache缓存，用时才读入数据到内存
    if(page_num>=TABLE_MAX_PAGES){
        printf("Tried to fetch page number out of bounds.\n");
        exit(EXIT_FAILURE);
    }
    if(pager->pages[page_num]==NULL){
        void* page= malloc(PAGE_SIZE);
        u_int32_t num_pages=pager->file_length/PAGE_SIZE;
        if(pager->file_length%PAGE_SIZE){
            num_pages+=1;
        }
        if(page_num<=num_pages){
            lseek(pager->file_descriptor,page_num*PAGE_SIZE,SEEK_SET);
            ssize_t bytes_read= read(pager->file_descriptor,page,PAGE_SIZE);
            if(bytes_read==-1){
                printf("Error reading file.\n");
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num]=page;
    }
    return pager->pages[page_num];
}
//TODO main
int main(int argc,char*argv[]) {
    if(argc<2){
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }
    char* filename=argv[1];
    Table* table = db_open(filename);
    InputBuffer* input_buffer=new_input_buffer();
    while(true){
        print_prompt();
        read_input(input_buffer);
        //检查是否是退出之类的元指令
        if(input_buffer->buffer[0] == '.'){
            switch (do_meta_command(input_buffer,table)) {
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
