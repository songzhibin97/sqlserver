package sqlservers

import (
	"database/sql"
	"fmt"
	"github.com/360EntSecGroup-Skylar/excelize"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/sirupsen/logrus"
	"os"
	"strconv"
	"strings"
	"time"
)

// ============= 配置信息
type Config struct {
	Server   string `ini:"server"`
	Database string `ini:"database"`
	UserId   string `ini:"userId"`
	Password string `ini:"password"`
	Port     int    `ini:"port"`
}
type TimeConfig struct {
	StartTime string `ini:"startTime"`
	EndTime   string `ini:"endTime"`
}

type LogLever struct {
	Lever string `ini:"lever"`
}

// ========== wms需要接收到到保存的信息 一条任务
type WmsTaskBk struct {
	taskPallet   string         // 板材编号
	palletWeight string         // 拣板数量
	createTime   *time.Time     // 任务创建时间
	startMove    *time.Time     // 开始移动时间
	arriveTime   *time.Time     // 到达拣板工位时间
	startPicking *time.Time     // 开始拣板时间
	overPicking  *time.Time     // 结束拣板时间
	finishTime   *time.Time     // 任务结束时间
	averageTime  *time.Duration // 平均拣板时间
	handling     *time.Duration // 搬送时间
	filed1       string         // 组合批次号
	filed3       string         // 拣板工位名
}

// ========== wcs报文
type WcsMsg struct {
	MessageType    int
	MessageTypeWcs int
	MessageTypePLC int
	saveTime       *time.Time
}

// ========== 以组合批次区分
type combinedPallet map[string][]*WmsTaskBk

// ========== 以区域划分
type regionalPallet map[string][]*WmsTaskBk

// 使用到的变量
var (
	wmsClient   *sql.DB        // 初始化后连接wms数据库句柄
	wcsClient   *sql.DB        // 初始化后连接wcs数据库句柄
	TimeConfigs *TimeConfig    // 初始化读取配置文件获取 起止日期
	logF        *logrus.Logger // 日志文件句柄
	logModel    logrus.Level
)

// 初始化连接
func Init(wmsConfig, wcsConfig *Config, timeConfigs *TimeConfig, logLever *LogLever) {
	// 创建日志句柄
	logF = logrus.New()
	switch logLever.Lever {
	case "Trace":
		logModel = logrus.TraceLevel
	case "Debug":
		logModel = logrus.DebugLevel
	case "Info":
		logModel = logrus.InfoLevel
	case "Warn":
		logModel = logrus.WarnLevel
	case "Error":
		logModel = logrus.ErrorLevel
	case "Fatal":
		logModel = logrus.FatalLevel
	case "Panic":
		logModel = logrus.PanicLevel
	default:
		logModel = logrus.DebugLevel
	}
	logF.Level = logModel
	now := time.Now().Format("2006-01-02")
	file, err := os.OpenFile(fmt.Sprint(now, ".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0755)
	logF.Out = file
	// 创建两条dns
	wmsDns := fmt.Sprintf("server=%s;database=%s;user id=%s;password=%s;port=%d;encrypt=disable",
		wmsConfig.Server, wmsConfig.Database, wmsConfig.UserId, wmsConfig.Password, wmsConfig.Port)
	wcsDns := fmt.Sprintf("server=%s;database=%s;user id=%s;password=%s;port=%d;encrypt=disable",
		wcsConfig.Server, wcsConfig.Database, wcsConfig.UserId, wcsConfig.Password, wcsConfig.Port)
	logF.Debug("WmsDns:", wmsDns)
	logF.Debug("WcsDns:", wcsDns)
	// 初始化
	wmsClient, err = sql.Open("mssql", wmsDns)
	if err != nil {
		logF.Panic("mssql初始化失败->由于wmsDns异常", err)
	}
	err = wmsClient.Ping()
	if err != nil {
		logF.Panic("wms数据库连接失败", err)
	}
	wcsClient, err = sql.Open("mssql", wcsDns)
	if err != nil {
		logF.Panic("mssql初始化失败->由于wcsDns异常", err)
	}
	err = wcsClient.Ping()
	if err != nil {
		logF.Panic("wcs数据库连接失败", err)
	}
	TimeConfigs = timeConfigs
	fmt.Println("数据库初始化成功")
	logF.Debug("数据库初始化成功")
}

// 多行查询托盘 传入 起始日期,截止日期 返回切片 []*WmsTaskBk
func QuerySearchPallet(args ...interface{}) ([]*WmsTaskBk, error) {
	// 声明结构体变量 以及切片
	var list []*WmsTaskBk
	sqlString := `SELECT 
      [Task_Pallet]
      ,[Pallet_Weight]
      ,[Create_Time]
      ,[Finish_Time]
      ,[Field1]
      ,[Field3]
  FROM [WMS_1180].[dbo].[WMS_Task_BK]
  where Finish_Time between ? and ? and Des_Area in ('SelectRobotRight1','SelectRobotRight2','SelectRobotLeft1','SelectRobotLeft2') order by Finish_Time asc `
	// 获取多行数据
	// sql预处理
	stmt, err := wmsClient.Prepare(sqlString)
	if err != nil {
		logF.Error("WMS 数据库预处理error:", err)
		return list, err
	}
	rows, err := stmt.Query(args...)
	if err != nil {
		logF.Error("WMS 数据库获取多行数据error:", err)
		return list, err
	}
	defer rows.Close()
	// 循环取值 如果rows还有值则Next方法返回True
	for rows.Next() {
		wmstask := new(WmsTaskBk)
		err = rows.Scan(&wmstask.taskPallet, &wmstask.palletWeight, &wmstask.createTime,
			&wmstask.finishTime, &wmstask.filed1, &wmstask.filed3)
		if err != nil {
			logF.Error("WMS 循环多行数据error", err)
			return list, err
		}
		wmstask.palletWeight = strings.TrimSpace(wmstask.palletWeight)
		// 添加至切片中
		list = append(list, wmstask)
	}
	// 返回切片列表
	fmt.Println("已成功查询", TimeConfigs.StartTime, "-", TimeConfigs.EndTime, "搬送托盘")
	logF.Debug("已成功查询", TimeConfigs.StartTime, "-", TimeConfigs.EndTime, "搬送托盘")
	return list, nil

}

// 划分分区 按照组合批次号划分
func PartitionPallet(list []*WmsTaskBk) (combinedPallets combinedPallet) {
	combinedPallets = make(combinedPallet)
	fmt.Println("正在按照组合批次号进行数据划分")
	logF.Debug("正在按照组合批次号进行数据划分")
	for _, wmsTaskBk := range list {
		QuerySearchMessage(wmsTaskBk)
		Efficiency(wmsTaskBk)
		//fmt.Printf("%T\n",wmsTaskBk)
		// 循环判断是否存在分区 如果存在则加入对应切片 如果不存在创建
		_, ok := combinedPallets[wmsTaskBk.filed1]
		if !ok {
			// 新增分区
			combinedPallets[wmsTaskBk.filed1] = []*WmsTaskBk{wmsTaskBk}
			continue
		}
		combinedPallets[wmsTaskBk.filed1] = append(combinedPallets[wmsTaskBk.filed1], wmsTaskBk)
	}
	fmt.Println("已成功解析报文", TimeConfigs.StartTime, "-", TimeConfigs.EndTime, "对应托盘")
	logF.Debug(fmt.Sprint("已成功解析报文", TimeConfigs.StartTime, "-", TimeConfigs.EndTime, "对应托盘"))

	return
}

// 划分分区 按照区域划分
func RegionalPallet(list []*WmsTaskBk) (regionalPallets regionalPallet) {
	fmt.Println("正在按照区域进行数据划分")
	logF.Debug("正在按区域进行数据划分")
	regionalPallets = make(regionalPallet)
	for _, wmsTaskBk := range list {
		QuerySearchMessage(wmsTaskBk)
		Efficiency(wmsTaskBk)
		// 循环判断是否存在分区 如果存在则加入对应切片 如果不存在创建
		_, ok := regionalPallets[wmsTaskBk.filed3]
		if !ok {
			// 新增分区
			regionalPallets[wmsTaskBk.filed3] = []*WmsTaskBk{wmsTaskBk}
			continue
		}
		regionalPallets[wmsTaskBk.filed3] = append(regionalPallets[wmsTaskBk.filed3], wmsTaskBk)
	}
	fmt.Println("已成功解析报文", TimeConfigs.StartTime, "-", TimeConfigs.EndTime, "对应托盘")
	logF.Debug(fmt.Sprintln("已成功解析报文", TimeConfigs.StartTime, "-", TimeConfigs.EndTime, "对应托盘"))
	return
}

// 安装托盘信息查找对应报文 加入 startMove arriveTime startPicking startPicking overPicking 计算 averageTime
func QuerySearchMessage(wmsTaskBk *WmsTaskBk) {
	sqlString := `SELECT 
	   [MessageType]
      ,[MessgeTypeWCS]
      ,[MessageTypePLC]
      ,[SaveTime]
  FROM [WCS_1180].[dbo].[Telegrame] where GoodsUnitCode = ? and SaveTime between ? and ? order by SaveTime asc`
	// 获取多行数据
	// sql预处理
	stmt, err := wcsClient.Prepare(sqlString)
	if err != nil {
		logF.Error("WCS 数据库预处理error:", err)
		return
	}
	rows, err := stmt.Query(wmsTaskBk.taskPallet, wmsTaskBk.createTime, wmsTaskBk.finishTime)
	if err != nil {
		logF.Error("WCS 数据库获取多行数据error:", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		wcsMsg := new(WcsMsg)
		err = rows.Scan(&wcsMsg.MessageType, &wcsMsg.MessageTypeWcs, &wcsMsg.MessageTypePLC, &wcsMsg.saveTime)
		if err != nil {
			logF.Error("WCS循环多行数据error:", err)
			return
		}
		switch {
		case wcsMsg.MessageType == 1 && wcsMsg.MessageTypeWcs == 1:
			wmsTaskBk.startMove = wcsMsg.saveTime
		case wcsMsg.MessageType == 1002 && wcsMsg.MessageTypePLC == 25:
			wmsTaskBk.arriveTime = wcsMsg.saveTime
		case wcsMsg.MessageType == 2 && wcsMsg.MessageTypeWcs == 3:
			wmsTaskBk.startPicking = wcsMsg.saveTime
		case wcsMsg.MessageType == 1002 && wcsMsg.MessageTypePLC == 26:
			wmsTaskBk.overPicking = wcsMsg.saveTime
		}
	}
}

// 计算拣板的平均时间以及搬送时间
func Efficiency(wmsTaskBk *WmsTaskBk) {
	if wmsTaskBk.startPicking != nil {
		res := wmsTaskBk.overPicking.Sub(*wmsTaskBk.startPicking)
		number, err := strconv.ParseInt(wmsTaskBk.palletWeight, 10, 64)
		if err != nil {
			logF.Error("拣板数量转换失败:托盘号:", wmsTaskBk.taskPallet)
			return
		}
		r := res / time.Duration(number)
		wmsTaskBk.averageTime = &r
	}
	if wmsTaskBk.startMove != nil && wmsTaskBk.arriveTime != nil {
		handTime :=wmsTaskBk.arriveTime.Sub(*wmsTaskBk.startMove)
		wmsTaskBk.handling = &handTime
	}
}

func Close() {
	defer wmsClient.Close()
	defer wcsClient.Close()
}

func ExportExcel(rangeMap map[string][]*WmsTaskBk, timePostfix string) {
	// 创建excel
	f := excelize.NewFile()
	rows := 'A'
	var nowTitle string
	for title, pointValue := range rangeMap {
		offset := 0
		// 创建一个工作表
		f.NewSheet(title)

		// 设置行高
		f.SetRowHeight(title, 1, 20)
		// 设置宽度
		f.SetColWidth(title, "A", "B", 10)
		f.SetColWidth(title, "C", "K", 20)
		// 设置 表头
		f.SetCellValue(title, "A1", title)
		f.SetCellValue(title, "A2", "板材编号")
		f.SetCellValue(title, "B2", "拣板数量")
		f.SetCellValue(title, "C2", "任务创建时间")
		f.SetCellValue(title, "D2", "开始移动时间")
		f.SetCellValue(title, "E2", "到达拣板工位时间")
		f.SetCellValue(title, "F2", "开始拣板时间")
		f.SetCellValue(title, "G2", "结束拣板时间")
		f.SetCellValue(title, "H2", "任务结束时间")
		f.SetCellValue(title, "I2", "拣板工位名")
		f.SetCellValue(title, "J2", "组合批次号")
		f.SetCellValue(title, "K2", "平均拣板时间(秒/张)")
		f.SetCellValue(title, "L2", "搬送时间")
		for index, value := range pointValue {
			if nowTitle != value.filed1 {
				offset++
				nowTitle = value.filed1
			}
			//taskPallet   string    // 板材编号
			//palletWeight string         // 拣板数量
			//createTime   *time.Time     // 任务创建时间
			//startMove    *time.Time     // 开始移动时间
			//arriveTime   *time.Time     // 到达拣板工位时间
			//startPicking *time.Time     // 开始拣板时间
			//overPicking  *time.Time     // 结束拣板时间
			//finishTime   *time.Time     // 任务结束时间
			//averageTime  *time.Duration // 平均拣板时间
			//filed1       string         // 组合批次号
			//filed3       string         // 拣板工位名
			// 设置行高
			f.SetRowHeight(title, index+3, 20)
			f.SetCellValue(title, fmt.Sprint(string(rows), index+3+offset), value.taskPallet)
			f.SetCellValue(title, fmt.Sprint(string(rows+1), index+3+offset), value.palletWeight)
			f.SetCellValue(title, fmt.Sprint(string(rows+2), index+3+offset), value.createTime)
			f.SetCellValue(title, fmt.Sprint(string(rows+3), index+3+offset), value.startMove)
			f.SetCellValue(title, fmt.Sprint(string(rows+4), index+3+offset), value.arriveTime)
			f.SetCellValue(title, fmt.Sprint(string(rows+5), index+3+offset), value.startPicking)
			f.SetCellValue(title, fmt.Sprint(string(rows+6), index+3+offset), value.overPicking)
			f.SetCellValue(title, fmt.Sprint(string(rows+7), index+3+offset), value.finishTime)
			f.SetCellValue(title, fmt.Sprint(string(rows+8), index+3+offset), value.filed3)
			f.SetCellValue(title, fmt.Sprint(string(rows+9), index+3+offset), value.filed1)
			f.SetCellValue(title, fmt.Sprint(string(rows+10), index+3+offset), value.averageTime)
			f.SetCellValue(title, fmt.Sprint(string(rows+11), index+3+offset), value.handling)

		}

		f.SetActiveSheet(0)
		// 根据指定路径保存文件
		if err := f.SaveAs("analysis-" + timePostfix + ".xlsx"); err != nil {
			println(err.Error())
		}
		fmt.Println("保存成功")
		logF.Debug("Excel保存成功")
	}
}
