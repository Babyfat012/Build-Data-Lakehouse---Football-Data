to_mysql:
	docker exec -it de_mysql mysql -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE}

to_mysql_root:
	docker exec -it de_mysql mysql -u"root" -p"${MYSQL_ROOT_PASSWORD}" ${MYSQL_DATABASE}

mysql_create:
	docker exec -it de_mysql mysql --local-infile -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE} -e"source /tmp/load_dataset_to_mysql/football_db.sql"

mysql_load:
	docker exec -it de_mysql mysql --local-infile -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE} -e"source /tmp/load_dataset_to_mysql/load_data.sql"


----SHOW GLOBAL VARIABLES LIKE 'local_infile';
---SET GLOBAL local_infile = 1;
# mc alias set minio http://minio:9000 minio minio123
--- docker exec -it mc sh
