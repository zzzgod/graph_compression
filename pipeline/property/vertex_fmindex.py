import os
import pandas as pd
import zstandard as zstd
import sys

def add_project_root():
    file_path = os.path.dirname(os.path.abspath(__file__))
    while file_path != os.path.dirname(file_path):
        if file_path not in sys.path:
            sys.path.append(file_path)
        file_path = os.path.dirname(file_path)

add_project_root()


from pipeline.property.fmindex import FMIndex
from config import vertex_block_size, vertex_bwt_block_size, project_root, stop_char

def process_vertex_fmindex():
    # 读取 node_property.csv 文件
    node_property_path = os.path.join(project_root, 'data/preprocess/node_property.csv')
    text = ""
    with open(node_property_path, 'r') as f:
        # 跳过第一行标题
        next(f)
        for line in f:
            text += line.strip() + stop_char
    
    print("原始串长度:", len(text))
    
    # 生成 BWT
    fm_index = FMIndex(text, stop_char)
    bwt = fm_index.get_bwt()

    # 打印前10个字符
    print("bwt前10个字符:", bwt[:10])

    # 打印后缀数组前10个元素
    sa = fm_index.get_suffix_array()
    print("后缀数组前10个元素:", sa[:10])

    # 打印 C 数组前10个元素
    c_array = fm_index.get_c_array()
    print("C 数组前10个元素:")
    for key, value in list(c_array.items())[:10]:
        print(f"{key}: {value}")


if __name__ == "__main__":
    process_vertex_fmindex()
    print("压缩完成")
