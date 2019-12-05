>1:在配置docker环境下建造一个镜像
'''
     docker build -t das_flow:v2 .
'''
2:创建一个数据卷
'''
     docker volume create filesname
'''
3:建造成功运行这个容器,并且挂载一个主机目录作为数据卷source 表示需要挂载的主机目录，target表示容器中数据卷的位置
'''
    docker run -it --mount type=bind,source=/mnt/win/hegh/file/5.14,target=/code/das_flow/testfile/ -v  filesname:/code/das_flow/ das_flow:v7 
'''

