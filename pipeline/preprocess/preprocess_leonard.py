import pandas as pd
import os
import sys
import uuid


def add_project_root():
    file_path = os.path.dirname(os.path.abspath(__file__))
    while file_path != os.path.dirname(file_path):
        if file_path not in sys.path:
            sys.path.append(file_path)
        file_path = os.path.dirname(file_path)

add_project_root()
import config


def preprocess_vertex(src_file: str, dest_dir: str):
    """
    Preprocess vertices
    :return: None
    """
    df = pd.read_csv(src_file, low_memory=False)
    node_hash = df['hash']
    # 将hash转换为二进制格式
    binary_hash = node_hash.apply(lambda x: uuid.UUID(x).bytes)
    # 打印第一个uuid的二进制数据测试
    print("first uuid binary:", binary_hash[0])
    print("first uuid hex:", uuid.UUID(bytes=binary_hash[0]).hex)
    del df['hash']
    # 将二进制数据直接写入文件
    with open(os.path.join(dest_dir, 'node_id.bin'), 'wb') as f:
        for binary in binary_hash:
            f.write(binary)
    df.to_csv(os.path.join(dest_dir, 'node_property.csv'), index=False)
    return node_hash


# def preprocess_edge(src_file: str, dest_dir: str, node_hash):
#     """
#     Preprocess edges
#     :return: None
#     """
#     df = pd.read_csv(src_file, low_memory=False)
#     edge_hash = df[['parentVertexHash', 'childVertexHash']]
#     del_cols = ['hash', 'parentVertexHash', 'childVertexHash']
#     for i in del_cols:
#         del df[i]
#     edge_hash.to_csv(os.path.join(dest_dir, 'edge_id.csv'), index=False)
#     df.to_csv(os.path.join(dest_dir, 'edge_property.csv'), index=False)



def preprocess_edge(src_file: str, dest_dir: str, node_hash):
    """
    Preprocess edges
    :return: None
    """
    df = pd.read_csv(src_file, low_memory=False)
    # 获取边的hash列
    edge_hash = df[['parentVertexHash', 'childVertexHash']].copy()
    
    # 创建从节点hash到序号的映射
    node_order = {hash_val: idx for idx, hash_val in enumerate(node_hash)}
    
    # 将hash映射为序号
    edge_hash.loc[:, 'parent_order'] = edge_hash['parentVertexHash'].map(node_order)
    edge_hash.loc[:, 'child_order'] = edge_hash['childVertexHash'].map(node_order)
    
    # 按父节点序号和子节点序号排序
    edge_hash = edge_hash.sort_values(by=['parent_order', 'child_order'], kind='mergesort')
    
    # 只保留序号列
    edge_list = edge_hash[['parent_order', 'child_order']]
    
    # 将序号列保存为空格分隔的文本文件
    edge_list.to_csv(os.path.join(dest_dir, 'edge_id.txt'), 
                     sep=' ', 
                     index=False,
                     header=False)
    
    # 保存边的其他属性
    del_cols = ['hash', 'parentVertexHash', 'childVertexHash'] 
    for i in del_cols:
        del df[i]
    df.to_csv(os.path.join(dest_dir, 'edge_property.csv'), index=False)


def preprocess(vertex_src: str, edge_src: str, dest_dir: str):
    """
    Preprocess the Leonard dataset
    :return: None
    """
    node_hash = preprocess_vertex(vertex_src, dest_dir)
    preprocess_edge(edge_src, dest_dir, node_hash)


def preprocess_toy():
    vertex = os.path.join(config.project_root, 'data/raw/vertex200m.csv')
    edge = os.path.join(config.project_root, 'data/raw/edge200m.csv')
    dest = os.path.join(config.project_root, 'data/preprocess')
    preprocess(vertex, edge, dest)


def preprocess_exp():
    vertex = os.path.join(config.project_root, 'data/raw/exp_vertex.csv')
    edge = os.path.join(config.project_root, 'data/raw/exp_edge.csv')
    dest = os.path.join(config.project_root, 'data/preprocess')
    vertex = pd.read_csv(vertex, low_memory=False)
    edge = pd.read_csv(edge, low_memory=False)
    vertex.to_csv(os.path.join(dest, 'node_property.csv'), index=False)
    edge.to_csv(os.path.join(dest, 'edge_property.csv'), index=False)


if __name__ == '__main__':
    preprocess_toy()
