package de.hpi.socrata.crawl

object RecentCrawlSummaryMain extends App{
  val summarizer = new CrawlSummarizer(args(0),Some(args(1)))
  summarizer.recentCrawlSummary()

}
