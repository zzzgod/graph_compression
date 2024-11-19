import os  
import sys  
import zstandard as zstd  
import json  

def add_project_root():  
    file_path = os.path.dirname(os.path.abspath(__file__))  
    while file_path != os.path.dirname(file_path):  
        if file_path not in sys.path:  
            sys.path.append(file_path)  
        file_path = os.path.dirname(file_path)  

add_project_root()  

from config import project_root  # 确保 `config.py` 中定义了 `project_root`  

MARKER_INTERVAL = 1024  # 1KB   

def compress_with_zstd_anchors(data, marker_interval=MARKER_INTERVAL, compressed_file='compressed.zst', index_file='anchors.json'):  
    compressor = zstd.ZstdCompressor()  
    anchors = []  
    with open(os.path.join(project_root, compressed_file), 'wb') as comp_f:  
        position = 0  
        while position < len(data):  
            chunk = data[position:position + marker_interval]  
            block_bytes = chunk.encode('utf-8')  
            # 记录当前压缩文件的位置作为帧的起始点  
            anchors.append(comp_f.tell())  
            # 压缩当前块为独立的帧  
            compressed_block = compressor.compress(block_bytes)  
            comp_f.write(compressed_block)  
            position += marker_interval  
    # 保存锚点  
    with open(os.path.join(project_root, index_file), 'w') as idx_f:  
        json.dump(anchors, idx_f)  

def decompress_from_zstd_anchor(compressed_file, index_file, anchor_index, marker_interval=MARKER_INTERVAL):  
    decompressor = zstd.ZstdDecompressor()  
    with open(os.path.join(project_root, index_file), 'r') as idx_f:  
        anchors = json.load(idx_f)  
    if anchor_index >= len(anchors):  
        raise IndexError("锚点索引超出范围")  
    with open(os.path.join(project_root, compressed_file), 'rb') as comp_f:  
        comp_f.seek(anchors[anchor_index])  
        # 读取从锚点开始的压缩数据  
        compressed_block = comp_f.read()  
        try:  
            # 解压当前帧  
            decompressed_block = decompressor.decompress(compressed_block)  
        except zstd.ZstdError as e:  
            raise zstd.ZstdError(f"解压锚点 {anchor_index} 处的数据失败: {e}")  
    return decompressed_block.decode('utf-8')  

# 使用示例  
if __name__ == "__main__":  
    # 示例数据  
    data = "ABCDEFGHIJKLMNOPQRSTUVWXYZ\n" * 1000  # 大约26KB数据  
    # 压缩并记录锚点  
    compress_with_zstd_anchors(data, marker_interval=1024, compressed_file='compressed.zst', index_file='anchors.json')  
    # 从第2个锚点解压  
    recovered_data = decompress_from_zstd_anchor('compressed.zst', 'anchors.json', anchor_index=2)  
    print(recovered_data)