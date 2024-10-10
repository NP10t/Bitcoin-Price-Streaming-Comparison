from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Thông tin kết nối Cassandra
cassandra_host = 'localhost'  # Địa chỉ IP của container (localhost trong trường hợp này vì bạn đang chạy container local)
cassandra_port = 9042  # Port của Cassandra (9042 là port mặc định)
username = 'cassandra'  # Tên đăng nhập Cassandra
password = 'cassandra'  # Mật khẩu Cassandra

# Kết nối tới Cassandra
auth_provider = PlainTextAuthProvider(username=username, password=password)
cluster = Cluster([cassandra_host], port=cassandra_port, auth_provider=auth_provider)

# Tạo session kết nối
session = cluster.connect()

# Chọn keyspace
session.set_keyspace('spark_streams')

# Truy vấn lấy 10 first_name từ bảng created_users
query = "SELECT first_name FROM created_users LIMIT 10"
rows = session.execute(query)

# In kết quả
for row in rows:
    print(row.first_name)

# Đóng kết nối
cluster.shutdown()
