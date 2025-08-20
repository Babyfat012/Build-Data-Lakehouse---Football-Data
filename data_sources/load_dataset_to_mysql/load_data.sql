SET FOREIGN_KEY_CHECKS = 0;

LOAD DATA local INFILE '/tmp/database/competitions.csv'
INTO TABLE competitions FIELDS TERMINATED BY ',' enclosed BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA local INFILE '/tmp/database/clubs.csv'
INTO TABLE clubs FIELDS TERMINATED BY ',' enclosed BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA local INFILE '/tmp/database/games.csv'
INTO TABLE games FIELDS TERMINATED BY ',' enclosed BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
SHOW WARNINGS;

LOAD DATA local INFILE '/tmp/database/players.csv'
INTO TABLE players FIELDS TERMINATED BY ',' enclosed BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SET FOREIGN_KEY_CHECKS = 1;