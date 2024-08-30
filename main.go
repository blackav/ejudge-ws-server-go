package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"

	_ "github.com/go-sql-driver/mysql"
)

type Settings struct {
	MysqlHost     string
	MysqlPort     string
	MysqlUser     string
	MysqlPassword string
	MysqlDatabase string
	RedisHost     string
	RedisPort     string
	RedisPassword string
}

const USER_ROLE_CONTESTANT = 0
const USER_ROLE_OBSERVER = 1
const USER_ROLE_EXAMINER = 2
const USER_ROLE_CHIEF_EXAMINER = 3
const USER_ROLE_COORDINATOR = 4
const USER_ROLE_JUDGE = 5
const USER_ROLE_ADMIN = 6

type EjudgeAPIKey struct {
	Token       string
	UserID      int32
	ContestID   int32
	CreateTime  *time.Time
	ExpiryTime  *time.Time
	AllContests bool
	RoleID      int8
}

func GetEjudgeAPIKey(db *sql.DB, key string) (*EjudgeAPIKey, error) {
	ps, err := db.Prepare("SELECT token, user_id, contest_id, create_time, expiry_time, all_contests, role_id FROM `apikeys` WHERE `token` = ?")
	if err != nil {
		return nil, err
	}
	defer ps.Close()

	var token string
	var userID int32
	var contestID int32
	var createTime *time.Time
	var expiryTime *time.Time
	var allContests bool
	var roleID int8

	err = ps.QueryRow(key).Scan(&token, &userID, &contestID, &createTime, &expiryTime, &allContests, &roleID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return &EjudgeAPIKey{
		Token:       token,
		UserID:      userID,
		ContestID:   contestID,
		CreateTime:  createTime,
		ExpiryTime:  expiryTime,
		AllContests: allContests,
		RoleID:      roleID,
	}, nil
}

var PermissionDenied = errors.New("permission denied")

func EjudgeTokenAuthMiddleware(db *sql.DB) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		err := func() error {
			ah := ctx.Request.Header.Get("Authorization")
			if !strings.HasPrefix(strings.ToLower(ah), "bearer ") {
				return PermissionDenied
			}
			ah2 := ah[7:]
			if !strings.HasPrefix(ah2, "AQAA") {
				return PermissionDenied
			}
			token := ah2[4:]
			kk, err := GetEjudgeAPIKey(db, token)
			if err != nil {
				return err
			}
			if kk == nil {
				return PermissionDenied
			}
			if kk.RoleID != USER_ROLE_ADMIN {
				return PermissionDenied
			}
			if !kk.AllContests {
				return PermissionDenied
			}
			cur := time.Now()
			if kk.ExpiryTime != nil && cur.After(*kk.ExpiryTime) {
				return PermissionDenied
			}

			return nil
		}()
		if errors.Is(err, PermissionDenied) {
			ctx.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"ok":      false,
				"message": err.Error(),
			})
			return
		}
		if err != nil {
			ctx.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				"ok":      false,
				"message": "internal error",
			})
			log.Println("Error: ", err.Error())
			return
		}

		ctx.Next()
	}
}

var upgrader = websocket.Upgrader{}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Panic("failed to load .env file: ", err.Error())
	}

	settings := Settings{}
	settings.MysqlHost = os.Getenv("MYSQL_HOST")
	if settings.MysqlHost == "" {
		settings.MysqlHost = "localhost"
	}
	settings.MysqlPort = os.Getenv("MYSQL_PORT")
	if settings.MysqlPort == "" {
		settings.MysqlPort = "3306"
	}
	settings.MysqlDatabase = os.Getenv("MYSQL_DATABASE")
	if settings.MysqlDatabase == "" {
		settings.MysqlDatabase = "ejudge"
	}
	settings.MysqlUser = os.Getenv("MYSQL_USER")
	if settings.MysqlUser == "" {
		settings.MysqlUser = "ejudge"
	}
	settings.MysqlPassword = os.Getenv("MYSQL_PASSWORD")
	if settings.MysqlPassword == "" {
		log.Panic("MYSQL_PASSWORD undefined")
	}

	connstr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", settings.MysqlUser, settings.MysqlPassword, settings.MysqlHost, settings.MysqlPort, settings.MysqlDatabase)
	db, err := sql.Open("mysql", connstr)
	if err != nil {
		log.Panic("cannot connect to mysql: ", err.Error())
	}

	settings.RedisHost = os.Getenv("REDIS_HOST")
	if settings.RedisHost == "" {
		settings.RedisHost = "localhost"
	}
	settings.RedisPort = os.Getenv("REDIS_PORT")
	if settings.RedisPort == "" {
		settings.RedisPort = "6379"
	}
	settings.RedisPassword = os.Getenv("REDIS_PASSWORD")

	redisAddr := fmt.Sprintf("%s:%s", settings.RedisHost, settings.RedisPort)
	log.Println("redis addr: ", redisAddr)
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: settings.RedisPassword,
		DB:       0,
	})

	router := gin.Default()
	router.Use(EjudgeTokenAuthMiddleware(db))
	router.GET("/test", func(ctx *gin.Context) {
		ctx.JSON(http.StatusOK, gin.H{
			"ok": true,
		})
	})
	router.GET("/ws/:stream/:serial", func(ctx *gin.Context) {
		stream := ctx.Param("stream")
		serial := ctx.Param("serial")
		conn, err := upgrader.Upgrade(ctx.Writer, ctx.Request, nil)
		if err != nil {
			log.Println("failed to upgrade connection: ", err)
			ctx.AbortWithError(http.StatusInternalServerError, err)
			return
		}

		log.Println("Stream: ", stream)
		log.Println("Serial: ", serial)

		for {
			res := rdb.XRead(context.Background(), &redis.XReadArgs{
				Streams: []string{stream, serial},
			})
			if res.Err() != nil {
				log.Println("redis read error: ", res.Err().Error())
				break
			}

			val := res.Val()
			if len(val) == 0 {
				continue
			}
			messages := val[0].Messages
			if len(messages) == 0 {
				continue
			}
			serial = messages[len(messages)-1].ID

			msg, err := json.Marshal(messages)
			if err != nil {
				log.Println("json error: ", err)
				break
			}

			err = conn.WriteMessage(websocket.TextMessage, msg)
			if err != nil {
				log.Println("websocket write error: ", err)
				break
			}
		}
	})
	router.Run()
}
