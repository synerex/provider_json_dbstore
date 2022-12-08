package main

import (
	// "encoding/json"
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v4/pgxpool"
	pb "github.com/synerex/synerex_api"

	nodeapi "github.com/synerex/synerex_nodeapi"

	sxutil "github.com/synerex/synerex_sxutil"
	//sxutil "local.packages/synerex_sxutil"

	"log"
	"sync"
)

type LatLon struct {
	Lat   float64
	Lon   float64
	Angle float64
}

var (
	channel         = flag.String("channel", "15", "Channel for input")
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local           = flag.String("local", "", "Local Synerex Server")
	posMap          map[int32]LatLon
	mu              sync.Mutex
	sxServerAddress string
	supplyClt       *sxutil.SXServiceClient
	db              *pgxpool.Pool
	db_host         = os.Getenv("POSTGRES_HOST")
	db_name         = os.Getenv("POSTGRES_DB")
	db_user         = os.Getenv("POSTGRES_USER")
	db_pswd         = os.Getenv("POSTGRES_PASSWORD")
	csv_columns     = [...]string{
		"IDENTIFIER", // 0: sid
		"DATE",       // 1: time
		"TIME",       // 2: time
		"LATITUDE",   // 3: lat
		"LONGITUDE",  // 4: lon
		"ALTITUDE",   // 5: alt
		"SPEED",      // 6: spd
		"COURSE",
		"NO2",
		"NO2_LIFE",
		"PM1",
		"PM25",
		"PM4",
		"PM10",
		"PM05#",
		"PM1#",
		"PM25#",
		"PM4#",
		"PM10#",
		"PM_SIZE",
		"S_CO2",
		"S_VOC",
		"S_H2,S_ETHANOL",
		"S_HUMIDITY",
		"S_TEMPERATURE",
		"S_ABS_HUMIDITY",
		"S_HEAT_INDEX",
		"S_DEW_POINT,PRESSURE",
		"TEMPERATURE",
		"HUMIDITY",
		"O_TEMPERATURE",
		"O_HUMIDITY",
		"O_ILLUMINANCE",
		"O_UV",
		"O_PRESSURE",
		"O_NOISE",
		"O_FUKAI",
		"O_WBGT",
		"O_BATT",
		"RSSI", // 39: rssi
		"SENSOR_STATUS",
	}
	// for on board devices
	carPrefix = [...]string{
		"NisshinEisei-OBD-",
		"HinodeEisei-OBD-",
		"Nikkan-OBD-",
		"ToyotaEisei-OBD-",
	}
	// for sensor devices
	sensors = [...]string{
		"600002",
		"600003",
		"600004",
		"600006",
		"600009",
		"600010",
		"600011",
		"600012",
		"600020",
		"600021",
		"600022",
		"600023",
		"600024",
		"600025",
	}
)

const layout = "2006-01-02T15:04:05.999999Z"
const layout_db = "2006-01-02 15:04:05.999"

func init() {
	posMap = make(map[int32]LatLon)

	// connect
	ctx := context.Background()
	addr := fmt.Sprintf("postgres://%s:%s@%s:5432/%s", db_user, db_pswd, db_host, db_name)
	print("connecting to " + addr + "\n")
	var err error
	db, err = pgxpool.Connect(ctx, addr)
	if err != nil {
		print("connection error: ")
		log.Println(err)
		log.Fatal("\n")
	}
	defer db.Close()

	// ping
	err = db.Ping(ctx)
	if err != nil {
		print("ping error: ")
		log.Println(err)
		log.Fatal("\n")
	}

	// create table (Location History)
	_, err = db.Exec(ctx, `create table if not exists lh(sid INT not null, time TIMESTAMP not null, lat DOUBLE PRECISION NOT NULL DEFAULT 0, lon DOUBLE PRECISION NOT NULL DEFAULT 0, alt DOUBLE PRECISION NOT NULL DEFAULT 0, spd DOUBLE PRECISION NOT NULL DEFAULT 0, acc DOUBLE PRECISION NOT NULL DEFAULT 0, dir_s DOUBLE PRECISION NOT NULL DEFAULT 0, dir_a DOUBLE PRECISION NOT NULL DEFAULT 0, rssi DOUBLE PRECISION NOT NULL DEFAULT 0, opt VARCHAR(4096) NOT NULL DEFAULT '')`)
	// select hex(mac) from log;
	// insert into pc (mac) values (x'000CF15698AD');
	if err != nil {
		print("create table error: ")
		log.Println(err)
		log.Fatal("\n")
	}

	_, err = db.Exec(ctx, `SELECT create_hypertable('lh', 'time', migrate_data => true, if_not_exists => true, chunk_time_interval => INTERVAL '15 mins')`)
	if err != nil {
		print("create_hypertable error: ")
		log.Println(err)
		log.Fatal("\n")
	}

	_, err = db.Exec(ctx, `SELECT attach_tablespace('fast_space', 'lh', if_not_attached => true)`)
	if err != nil {
		print("attach_tablespace error: ")
		log.Println(err)
		log.Fatal("\n")
	}

	_, err = db.Exec(ctx, `CLUSTER lh USING lh_time_idx`)
	if err != nil {
		print("CLUSTER error: ")
		log.Println(err)
		log.Fatal("\n")
	}

	var count int
	if err := db.QueryRow(ctx, `select count(*) from timescaledb_information.jobs where proc_name like 'move_old_chunks' and config?'hypertable' and config->>'hypertable' like 'lh'`).Scan(&count); err != nil {
		print("select count(*) from timescaledb_information.jobs query error: ")
		log.Println(err)
		print("\n")
	} else {
		if count == 0 {
			_, err = db.Exec(ctx, `SELECT add_job('move_old_chunks', '5m', config => '{"hypertable":"lh","lag":"1 hour","tablespace":"slow_space"}')`)
			if err != nil {
				print("SELECT add_job: ")
				log.Println(err)
				log.Fatal("\n")
			}
		}
	}
}

func dbStore(sid uint32, time_string string, lat float64, lon float64, alt float64, spd float64, dir_s float64, rssi float64, opt string) {

	// ping
	ctx := context.Background()
	err := db.Ping(ctx)
	if err != nil {
		print("ping error: ")
		log.Println(err)
		print("\n")
		// connect
		addr := fmt.Sprintf("postgres://%s:%s@%s:5432/%s", db_user, db_pswd, db_host, db_name)
		print("connecting to " + addr + "\n")
		db, err = pgxpool.Connect(ctx, addr)
		if err != nil {
			print("connection error: ")
			log.Println(err)
			print("\n")
		}
	}

	// log.Printf("Storeing %v, %s, %s, %d, %s, %d", ts.Format(layout_db), hexmac, hostname, sid, dir, height)
	// result, err := db.Exec(ctx, `insert into lh(sid, time, lat, lon, alt, spd, acc, dir_s, dir_a, rssi, opt) values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`, sid, ts.Format(layout_db), nummac, hostname, dir, height)
	result, err := db.Exec(ctx, `insert into lh(sid, time, lat, lon, alt, spd, dir_s, rssi, opt) values($1, $2, $3, $4, $5, $6, $7, $8, $9)`, sid, time_string, lat, lon, alt, spd, dir_s, rssi, opt)

	if err != nil {
		print("exec error: ")
		log.Println(err)
		print("\n")
	} else {
		rowsAffected := result.RowsAffected()
		if err != nil {
			log.Println(err)
		} else {
			print(rowsAffected)
		}
	}

}

// callback for each Supplu
func supplyCallback(clt *sxutil.SXServiceClient, sp *pb.Supply) {
	// check if demand is match with my supply.
	//	log.Printf("supplyCallback0")
	if sp.GetSupplyName() == "stdin" { // for usual data
		datastr := sp.GetArgJson() // ここで csv データを取得
		token := strings.Split(datastr, ",")
		//		log.Printf("get supply tokens %d",len(token))

		var vehicle_id int32 = 0
		for _, prefix := range carPrefix {
			if strings.HasPrefix(token[0], prefix) {
				carnum := token[0][len(prefix):]
				cn, _ := strconv.Atoi(carnum)
				vehicle_id = int32(30000 + cn)
				break
			}
		}
		if vehicle_id == 0 { // other cars?
			for _, sname := range sensors {
				if token[0] == sname {
					cn, _ := strconv.Atoi(sname)
					vehicle_id = int32(cn)
					break
				}
			}
		}

		// time parser
		time_string := fmt.Sprintf("%s %s", token[1], token[2])

		if vehicle_id == 0 {
			log.Printf("IngoreData:", datastr)
			return
		}
		lat, err := strconv.ParseFloat(token[3], 64)
		if err != nil || lat < 20.0 || lat > 46.0 {
			log.Printf("Err lat %s", token[3])
			//	log.Printf("err parse lat [%s] %s", token[3], err.Error())
			return
		}
		lon, err2 := strconv.ParseFloat(token[4], 64)
		if err2 != nil || lon < 122.0 || lon > 154.0 {
			log.Printf("Err lon %s", token[4])
			//			log.Printf("err parse lon [%s] %s", token[4], err.Error())
			return
		}
		altitude, _ := strconv.ParseFloat(token[5], 64)
		speed, _ := strconv.ParseFloat(token[6], 64)
		rssi, _ := strconv.ParseFloat(token[39], 64)

		lastPos, ok := posMap[vehicle_id]
		var angle, dist float64
		if ok { // まあだいたいでいいよね？ (北緯35 度の1度あたりの距離で計算)
			dlat := (lastPos.Lat - lat) * 110940.5844 // 110km / degree
			dlon := (lastPos.Lon - lon) * 91287.7885  // 91km / degree
			// あまりに移動が小さい場合は、考える。
			dist = math.Sqrt(dlat*dlat + dlon*dlon)
			if speed > 0.1 && dist > 2 { // need to fix
				angle = math.Atan2(dlon, dlat)*180/math.Pi + 180
			} else {
				angle = lastPos.Angle
			}
		} else {
			dist = 0
			angle = 0
		}
		lastPos.Lat = lat
		lastPos.Lon = lon
		lastPos.Angle = angle
		posMap[vehicle_id] = lastPos

		log.Printf("CarNUm:%6d, %f, %f, alt:%.2f spd:%.2f dst:%.2f agl %.2f ", vehicle_id, lat, lon, altitude, speed, dist, angle)

		if lat < 30 || lat > 40 || lon < 120 || lon > 150 {
			log.Printf("error too big!")
			return
		}

		dbStore(uint32(vehicle_id), time_string, lat, lon, altitude, speed, angle, rssi, "{}")
	}
}

// supply for channel data
func subscribeSupply(client *sxutil.SXServiceClient) {
	// goroutine!
	ctx := context.Background() //
	client.SubscribeSupply(ctx, supplyCallback)
	// comes here if channel closed
	log.Printf("Server closed... on conv_packer provider")
}

func main() {
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)
	log.Printf("Nisshin-Packer-dbstore(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)

	ch, cherr := strconv.Atoi(*channel)
	if cherr != nil {
		log.Fatal("Can't convert channel num ", *channel)
	}
	recvChan := uint32(ch)

	channelTypes := []uint32{recvChan}

	sxo := &sxutil.SxServerOpt{
		NodeType:   nodeapi.NodeType_PROVIDER,
		ServerInfo: "NisshinPackerDBStore",
		ClusterId:  0,
		AreaId:     "Default",
	}

	// obtain synerex server address from nodeserv
	srv, err := sxutil.RegisterNode(
		*nodesrv,
		"NisshinPackerDBStore",
		channelTypes,
		sxo,
	)

	if err != nil {
		log.Fatal("Can't register node...")
	}

	if *local != "" {
		sxServerAddress = *local
	} else {
		sxServerAddress = srv
	}
	log.Printf("Connecting Server [%s]\n", sxServerAddress)

	wg := sync.WaitGroup{} // for syncing other goroutines

	client := sxutil.GrpcConnectServer(sxServerAddress)

	if client == nil {
		log.Fatal("Can't connect Synerex Server")
	}

	argJson := fmt.Sprintf("{NisshinPackerDBStore:recv}")
	sclient := sxutil.NewSXServiceClient(client, recvChan, argJson)

	wg.Add(1)
	go subscribeSupply(sclient)
	wg.Wait()
	sxutil.CallDeferFunctions() // cleanup!

}
