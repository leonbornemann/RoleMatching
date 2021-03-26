package de.hpi.tfm.data.socrata.crawl

import de.hpi.tfm.util.CrawlSummarizer

object RecentCrawlSummaryMain extends App{
  val summarizer = new CrawlSummarizer(args(0),Some(args(1)))
  summarizer.recentCrawlSummary()

}
