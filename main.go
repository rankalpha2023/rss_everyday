package main

import (
	"RSS_bot/internal/dedup"
	"context"
	"crypto/md5"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/mmcdole/gofeed"
)

// 基础环境配置
var (
	BotToken       *string
	ChannelID      *int64
	StartBy        *int64
	RSSFilePath    *string
	DebugMode      *bool
	GoroutineNum   *int
	DigestFile     *string
	NoNewMsgNotify *bool
)

func TokenValid() {
	if *BotToken == "" || *ChannelID == 0 {
		panic("BotToken && ChannelID cannot be empty")
	}
}

func init() {
	BotToken = flag.String("tg-bot", "", "Telegram bot token")
	ChannelID = flag.Int64("tg-channel", 0, "Telegram channel id")
	StartBy = flag.Int64("startby", 6, "Start by specified time(hour)")
	RSSFilePath = flag.String("rss-filepath", "rss.json", "Rss json file path")
	DebugMode = flag.Bool("debug", false, "Debug mode")
	GoroutineNum = flag.Int("goroutine-num", 5, "Goroutine num")
	DigestFile = flag.String("digest-file", "digest_file.json", "digest file for deduplication")
	NoNewMsgNotify = flag.Bool("no-new-msg-notify", false, "notify when no new msg")
	flag.Parse()

	TokenValid()
	GetRssInfo()
}

// RSS 构成阶段
type RSSInfos struct {
	RssInfo []RssInfo `json:"rss_info"`
}

type RssInfo struct {
	Title       string `json:"title"`
	Url         string `json:"url"`
	FullContent bool   `json:"full_content"`
}

var RssInfos = RSSInfos{nil}

// 从 配置文件中获取 rss 链接
// 根据 rss 链接获取更新
func GetRssInfo() {
	rssFile, err := os.Open(*RSSFilePath)
	if err != nil {
		panic(err)
	}

	err = json.NewDecoder(rssFile).Decode(&RssInfos)
	if err != nil {
		panic(err)
	}

}

var (
	// 订阅 chan
	infoChan = make(chan RssInfo, 20)
	// 通知 tg chan
	tgChan = make(chan *gofeed.Item, 20)
)

// 根据时间筛选昨天一整天的文章
func InfoProducer(_ context.Context) {
	defer func() {
		close(infoChan)
	}()

	for _, info := range RssInfos.RssInfo {
		infoChan <- info
	}
}

func InfoComsumer(_ context.Context, done func()) {
	defer done()

	for info := range infoChan {
		feeds := GetPostInfo(info)
		// 发给 tg
		for _, feed := range feeds {
			tgChan <- feed
		}
	}
}

func debugInfof(fmt string, v ...interface{}) {
	if !(*DebugMode) {
		return
	}

	if !strings.HasSuffix(fmt, "\n") {
		fmt = fmt + "\n"
	}
	log.Printf("debug: "+fmt, v...)
}

// getDatetime 从左到右, 按优先级返回有效 datetime
// 实在没有, 返回最后一个时间
func getDatetime(times ...*time.Time) *time.Time {
	for _, d := range times {
		if d != nil && !d.IsZero() {
			return d
		}
	}
	return times[len(times)-1]
}

func GetPostInfo(rss RssInfo) []*gofeed.Item {
	var msg = make([]*gofeed.Item, 0)

	now := time.Now()
	startTime := now.Add(-(time.Duration(*StartBy) * time.Hour))
	start := startTime.Unix()
	end := now.Unix()

	maxRetries := 3
	var feed *gofeed.Feed
	var err error
	
	for i := 0; i < maxRetries; i++ {
		fp := gofeed.NewParser()
		feed, err = fp.ParseURL(rss.Url)
		if err == nil {
			break
		}
		log.Printf("Attempt %d: parse url err: url=%s, %v", i+1, rss.Url, err)
		time.Sleep(time.Duration(i+1) * time.Second)
	}
	
	if err != nil {
		log.Printf("Final error: parse url err: url=%s, %v", rss.Url, err)
		return msg
	}
	
	for _, item := range feed.Items {
		debugInfof("Title=%s, Url=%s, Published=%v, Updated=%v", item.Title, item.Link, item.Published, item.Updated)

		parseDatetime := getDatetime(item.PublishedParsed, item.UpdatedParsed)
		if parseDatetime != nil && parseDatetime.Unix() >= start && parseDatetime.Unix() < end {
			msg = append(msg, item)
		}
	}

	return msg
}

func safeExtractName(author *gofeed.Person) string {
	if author == nil {
		return ""
	}
	return fmt.Sprintf("%s\n", author.Name)
}

func makeDisplayMsg(item *gofeed.Item) string {
	return fmt.Sprintf(
		"%s%s\n%s",
		safeExtractName(item.Author),
		item.Title,
		item.Link,
	)
}

func initDeDuper() *dedup.DeDup[*gofeed.Item] {
	deduper, err := dedup.NewDeDup(*DigestFile, func(elem *gofeed.Item) string {
		content := ""
		if elem.Content != "" {
			content = elem.Content
		} else if elem.Description != "" {
			content = elem.Description
		}
		digest := md5.Sum([]byte(elem.Title + content + elem.Link))
		return fmt.Sprintf("%x", digest)
	})
	if err != nil {
		panic(fmt.Errorf("new dedup err: %v", err))
	}

	return deduper
}

func initTGBot() {
	onceLoader.Do(func() {
		if !*DebugMode {
			var err error
			bot, err = tgbotapi.NewBotAPI(*BotToken)
			if err != nil {
				panic(err)
			}
		}
	})
}

var (
	bot        *tgbotapi.BotAPI
	onceLoader sync.Once
)

// 从配置文件获取推送方式
// 使用对应的推送渠道推送文章
func PushPost(ctx context.Context, done func()) {
	defer done()

	// init bot
	initTGBot()
	// init deduper
	deduper := initDeDuper()

	cnt := 0
	for feed := range tgChan {
		// 1. dedplicate
		feeds := deduper.FilterMany([]*gofeed.Item{feed})
		if len(feeds) == 0 {
			continue
		}

		// 2. debug info
		info := fmt.Sprintln(feed.Title, feed.Link)
		log.Printf("%s", info)

		// 3. counter and sleep when condition is true
		cnt++
		if cnt%10 == 0 {
			time.Sleep(1 * time.Second)
		}

		// 4. do not send tg when is debug mode
		if *DebugMode {
			continue
		}

		// 5. send msg
		displayMsg := makeDisplayMsg(feed)
		if _, err := bot.Send(tgbotapi.NewMessage(*ChannelID, displayMsg)); err != nil {
			log.Printf("send tg err: %v\n", err)
		}
	}

	// 6. dump disk finally
	if err := deduper.MergeAndDump(*DigestFile); err != nil {
		log.Printf("merge and dump err: file=%s, %v", *DigestFile, err)
	}

	// 7. send alarm if need
	alarm(cnt)
}

// alarm send beat package when no new msg
func alarm(cnt int) {
	log.Printf("collect article num=%d", cnt)

	if *DebugMode {
		return
	}
	if cnt == 0 && *NoNewMsgNotify {
		if _, err := bot.Send(tgbotapi.NewMessage(*ChannelID, "😆only beat package, no new msg")); err != nil {
			log.Printf("send beat err: %v\n", err)
		}
	}
}

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	// PushPost
	go PushPost(ctx, cancel)

	// rss feed 订阅生产者
	go InfoProducer(context.Background())
	// rss feed 订阅的消费者
	var wg sync.WaitGroup
	wg.Add(*GoroutineNum)
	for i := 0; i < *GoroutineNum; i++ {
		go InfoComsumer(context.TODO(), wg.Done)
	}
	wg.Wait()

	log.Println("close tg chan")
	close(tgChan)
	log.Println("waiting for done")
	<-ctx.Done()
	log.Println("done ...")
}
