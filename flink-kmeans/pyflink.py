import time
import numpy as np
import matplotlib.pyplot as plt

from pyflink.table import TableEnvironment, EnvironmentSettings
from sklearn.cluster import KMeans

def main():
    #############################################
    # 1. 环境与时间初始化
    #############################################
    script_start_time = time.time()
    print("[INFO] Script start.")

    # 创建 PyFlink 批处理 TableEnvironment
    settings = EnvironmentSettings.in_batch_mode()
    table_env = TableEnvironment.create(settings)

    # CSV 文件路径（根据实际情况修改）
    input_csv_path = "file:///path/to/points.csv"          # 输入坐标文件
    output_csv_path = "file:///path/to/points_result.csv"  # 输出结果文件
    output_fig_path = "kmeans_result.png"                  # 保存可视化图片

    # 要分的簇数
    k = 2

    #############################################
    # 2. 创建输入表（CSV -> Table）
    #############################################
    print(f"[INFO] Defining input table from CSV: {input_csv_path}")
    table_env.execute_sql(f"""
    CREATE TABLE points (
        x DOUBLE,
        y DOUBLE
    ) WITH (
        'connector' = 'filesystem',
        'path' = '{input_csv_path}',
        'format' = 'csv'
    )
    """)

    points_table = table_env.from_path("points")

    #############################################
    # 3. 读取并转为 Pandas (触发 PyFlink 作业)
    #############################################
    t1 = time.time()
    points_df = points_table.to_pandas()
    t2 = time.time()
    print(f"[INFO] Collected {len(points_df)} rows into Pandas in {t2 - t1:.4f} seconds.")
    print("[INFO] Head of points_df:")
    print(points_df.head())

    # 如果 CSV 原本没有 cluster_id，可以后续添加
    # points_df.columns = ['x', 'y']  # 如果需要明确指定列名

    #############################################
    # 4. 使用 scikit-learn KMeans（本地计算）
    #############################################
    data_array = points_df[['x', 'y']].to_numpy()
    print(f"[INFO] Starting KMeans with k={k} ...")

    kmeans_start = time.time()
    kmeans = KMeans(n_clusters=k, random_state=42)
    kmeans.fit(data_array)
    kmeans_end = time.time()

    print(f"[INFO] KMeans fitting done in {kmeans_end - kmeans_start:.4f} seconds.")
    labels = kmeans.labels_
    centers = kmeans.cluster_centers_

    # 把结果添加进 DataFrame
    points_df['cluster_id'] = labels

    #############################################
    # 5. 写回结果到 CSV（并统计耗时）
    #############################################
    # 方式 A：直接用 Pandas 写出
    # points_df.to_csv("/path/to/points_result.csv", index=False, header=False)
    # print("[INFO] Results saved by pandas to 'points_result.csv'")

    # 方式 B：PyFlink Table 方式
    result_table = table_env.from_pandas(points_df)
    table_env.execute_sql(f"""
    CREATE TABLE points_result (
        x DOUBLE,
        y DOUBLE,
        cluster_id INT
    ) WITH (
        'connector' = 'filesystem',
        'path' = '{output_csv_path}',
        'format' = 'csv'
    )
    """)
    # 把带 cluster_id 的 Table 写到 CSV
    t3 = time.time()
    result_table.execute_insert("points_result")
    t4 = time.time()
    print(f"[INFO] Result table written to CSV in {t4 - t3:.4f} seconds: {output_csv_path}")

    #############################################
    # 6. 可视化聚类结果（matplotlib）
    #############################################
    plt.figure(figsize=(6, 6))
    for cluster_idx in range(k):
        cluster_points = points_df[points_df['cluster_id'] == cluster_idx]
        plt.scatter(cluster_points['x'], cluster_points['y'], label=f'Cluster {cluster_idx}')
    # 绘制 KMeans 中心（黑色 X）
    plt.scatter(centers[:, 0], centers[:, 1], s=200, c='black', marker='X', label='Centers')

    plt.title("KMeans Clustering Result")
    plt.xlabel("X")
    plt.ylabel("Y")
    plt.legend()
    plt.grid(True)
    plt.savefig(output_fig_path)
    print(f"[INFO] Result plot saved to {output_fig_path}")
    # 若在交互式环境可 plt.show()

    #############################################
    # 7. 统计总运行时间
    #############################################
    script_end_time = time.time()
    total_time = script_end_time - script_start_time
    print(f"[INFO] Script total time: {total_time:.4f} seconds.")
    print("[INFO] Done.")

if __name__ == "__main__":
    main()