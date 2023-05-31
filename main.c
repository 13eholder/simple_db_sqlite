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
typedef enum{
    NODE_INTERNAL,
    NODE_LEAF
} NodeType;
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
    u_int32_t num_pages;
    void* pages[TABLE_MAX_PAGES];
}Pager;
typedef struct {
    u_int32_t root_page_num;
    Pager* pager;
}Table;
typedef struct{
    Table* table;
    u_int32_t page_num;
    u_int32_t cell_num;
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
/*
    Common Node Header Layout
*/
const u_int32_t NODE_TYPE_SIZE=sizeof(u_int8_t); //结点类型 1字节
const u_int32_t NODE_TYPE_OFFSET=0;
const u_int32_t IS_ROOT_SIZE=sizeof(u_int8_t);//is_root 1字节
const u_int32_t IS_ROOT_OFFSET=NODE_TYPE_SIZE;
const u_int32_t PARENT_POINTER_SIZE=sizeof(u_int32_t);//祖先指针4字节
const u_int32_t PARENT_POINTER_OFFSET=IS_ROOT_OFFSET+IS_ROOT_SIZE;
const u_int8_t COMMON_NODE_HEADER_SIZE=NODE_TYPE_SIZE+IS_ROOT_SIZE+PARENT_POINTER_SIZE;
/*
    Leaf Node Header Layout
*/
const u_int32_t LEAF_NODE_NUM_CELLS_SIZE=sizeof(u_int32_t);
const u_int32_t LEAF_NODE_NUM_CELLS_OFFSET=COMMON_NODE_HEADER_SIZE;
const u_int32_t LEAF_NODE_HEADER_SIZE=COMMON_NODE_HEADER_SIZE+LEAF_NODE_NUM_CELLS_SIZE;
/*
    Leaf Node Body Layout
*/
const u_int32_t LEAF_NODE_KEY_SIZE=sizeof(u_int32_t);
const u_int32_t LEAF_NODE_KEY_OFFSET=0;
const u_int32_t LEAF_NODE_VALUE_SIZE=ROW_SIZE;
const u_int32_t LEAF_NODE_VALUE_OFFSET=LEAF_NODE_KEY_OFFSET+LEAF_NODE_KEY_SIZE;
const u_int32_t LEAF_NODE_CELL_SIZE=LEAF_NODE_KEY_SIZE+LEAF_NODE_VALUE_SIZE;
const u_int32_t LEAF_NODE_SPACE_FOR_CELLS=PAGE_SIZE-LEAF_NODE_HEADER_SIZE;
const u_int32_t LEAF_NODE_MAX_CELLS=LEAF_NODE_SPACE_FOR_CELLS/LEAF_NODE_CELL_SIZE;
//TODO 函数声明
// IO函数
InputBuffer * new_input_buffer();
void print_prompt();
void print_constants();
void print_leaf_node(void* node);
void read_input(InputBuffer*inputBuffer);
void close_input_buffer(InputBuffer* InputBuffer);
//语句解析
MetaCommandResult do_meta_command(InputBuffer* inputBuffer,Table* table);
PrepareResult prepare_insert(InputBuffer* inputBuffer,Statement* statement);
PrepareResult prepare_statement(InputBuffer* inputBuffer,Statement* statement);
ExecuteResult execute_insert(Statement*statement,Table*table);
ExecuteResult execute_select(Statement* statement,Table* table);
ExecuteResult execute_statement(Statement* statement,Table*table);
//序列化
void serialize_row(Row* source,void*destination);
void deserialize_row(void*source,Row*destination);
//初始化
Table* db_open(const char* filename);
void pager_flush(Pager*pager,u_int32_t page_num);
void db_close(Table* table);
Pager* pager_open(const char* filename);
void* get_page(Pager* pager,u_int32_t page_num);
void print_row(Row* row);
//游标
Cursor* table_start(Table* table);
Cursor* table_end(Table* table);
void* cursor_value(Cursor* cursor);
void cursor_advance(Cursor* cursor);
//叶节点
void leaf_node_insert(Cursor* cursor, u_int32_t key, Row* value);
//返回叶节点的cell数量
u_int32_t* leaf_node_num_cells(void* node){
    return node+LEAF_NODE_NUM_CELLS_OFFSET;
}
//返回 cell_num处的 cell
void* leaf_node_cell(void*node,u_int32_t cell_num){
    return node+LEAF_NODE_HEADER_SIZE+cell_num*LEAF_NODE_CELL_SIZE;
}
//返回 cell_num处的 cell_key
u_int32_t* leaf_node_key(void* node,u_int32_t cell_num){
    return leaf_node_cell(node, cell_num);
}
//
void* leaf_node_value(void* node,u_int32_t cell_num){
    return leaf_node_cell(node, cell_num)+LEAF_NODE_KEY_SIZE;
}
//将叶节点的cell_num 设为0
void initialize_leaf_node(void* node){
    *leaf_node_num_cells(node)=0;
}
//TODO 函数定义
void leaf_node_insert(Cursor* cursor, u_int32_t key, Row* value){
    void* node=get_page(cursor->table->pager, cursor->page_num);
    u_int32_t num_cells=*leaf_node_num_cells(node);
    if(num_cells>=LEAF_NODE_MAX_CELLS){
        printf("Need to implement splitting a leaf node.\n");
        exit(EXIT_FAILURE);
    }
    if(cursor->cell_num<num_cells){
        //移动cell
        for(u_int32_t i=num_cells;i>cursor->cell_num;i--){
            memcpy(leaf_node_cell(node,i),leaf_node_cell(node,i-1),LEAF_NODE_CELL_SIZE);            
        }
    }
    *(leaf_node_num_cells(node))+=1;
    *(leaf_node_key(node,cursor->cell_num))=key;
    serialize_row(value,leaf_node_value(node,cursor->cell_num));
}
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
    }else if (strcmp(inputBuffer->buffer,".constants")==0) {
        printf("Constants:\n");
        print_constants();
        return META_COMMAND_SUCCESS;
    }else if (strcmp(inputBuffer->buffer,".btree")==0) {
        printf("Tree:\n");
        print_leaf_node(get_page(table->pager,0));
        return META_COMMAND_SUCCESS;
    } else{
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
    void* node=get_page(table->pager, table->root_page_num);
    if((*leaf_node_num_cells(node))>=LEAF_NODE_MAX_CELLS){
        return EXECUTE_TABLE_FULL;
    }
    Row* row_to_insert=&(statement->row_to_insert);
    Cursor* cursor=table_end(table);
    leaf_node_insert(cursor,row_to_insert->id,row_to_insert);
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
    cursor->page_num=table->root_page_num;
    cursor->cell_num=0;

    void* root_node=get_page(table->pager, table->root_page_num);
    u_int32_t num_cells=*leaf_node_num_cells(root_node);
    cursor->end_of_table=(num_cells==0);
    return cursor;
}
//创建一个Cursor,指向表的结尾
Cursor* table_end(Table* table){
    Cursor* cursor =malloc(sizeof(Cursor));
    cursor->table=table;
    cursor->page_num=table->root_page_num;
    void* root_node=get_page(table->pager,table->root_page_num);
    cursor->cell_num=*leaf_node_num_cells(root_node);
    cursor->end_of_table=true;
    return cursor;
}
//返回一个 指向当前游标指向行的 指针
void* cursor_value(Cursor* cursor){
    u_int32_t page_num=cursor->page_num;
    void* page= get_page(cursor->table->pager,page_num);
    return leaf_node_value(page,cursor->cell_num);
}
//使游标指向下一行
void cursor_advance(Cursor* cursor){
    cursor->cell_num+=1;
    u_int32_t page_num=cursor->page_num;
    void* node=get_page(cursor->table->pager,page_num);
    if(cursor->cell_num>=(*leaf_node_num_cells(node))){
        cursor->end_of_table=true;
    }
}
//从文件中读取储存的数据库信息
Table* db_open(const char* filename){
    Pager* pager=pager_open(filename);
    Table* table= malloc(sizeof(Table));
    table->pager=pager;
    table->root_page_num=0;
    if(pager->num_pages==0){
        initialize_leaf_node(get_page(pager,0));
    }
    return table;
}
//关闭数据库，写回数据，释放内存
void db_close(Table* table){
    Pager* pager=table->pager;
    //写回数据
    for(u_int32_t i=0;i<pager->num_pages;i++){
        if(pager->pages[i]==NULL){
            continue;
        }
        pager_flush(pager,i);
        free(pager->pages[i]);
        pager->pages[i]=NULL;
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
void pager_flush(Pager*pager,u_int32_t page_num){
    if(pager->pages[page_num]==NULL){
        printf("Tried to flush null page.\n");
        exit(EXIT_FAILURE);
    }
    off_t offset= lseek(pager->file_descriptor,page_num*PAGE_SIZE,SEEK_SET);
    if(offset==-1){
        printf("Error seeking:%d\n",errno);
        exit(EXIT_FAILURE);
    }
    ssize_t bytes_written= write(pager->file_descriptor,pager->pages[page_num],PAGE_SIZE);
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
    pager->num_pages=file_length/PAGE_SIZE;
    if(file_length%PAGE_SIZE!=0){
        printf("Db file is not a whole number of pages. Corrupt file.\n");
        exit(EXIT_FAILURE);
    }
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
        if(page_num<=num_pages){
            lseek(pager->file_descriptor,page_num*PAGE_SIZE,SEEK_SET);
            ssize_t bytes_read= read(pager->file_descriptor,page,PAGE_SIZE);
            if(bytes_read==-1){
                printf("Error reading file.\n");
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num]=page;
        //一次载入page_num前缺失的所有页
        if(page_num>=pager->num_pages){
            pager->num_pages=page_num+1;
        }
    }
    return pager->pages[page_num];
}
void print_constants() {
  printf("ROW_SIZE: %d\n", ROW_SIZE);
  printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
  printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
  printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
  printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
  printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}
void print_leaf_node(void* node) {
  u_int32_t num_cells = *leaf_node_num_cells(node);
  printf("leaf (size %d)\n", num_cells);
  for (u_int32_t i = 0; i < num_cells; i++) {
    u_int32_t key = *leaf_node_key(node, i);
    printf("  - %d : %d\n", i, key);
  }
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
            switch (do_meta_command(input_buffer,table)){
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
