package de.hpi.dataset_versioning.crawl

import de.hpi.dataset_versioning.util.CrawlSummarizer

object RecentCrawlSummaryMain extends App{
  val summarizer = new CrawlSummarizer(args(0),Some(args(1)))
  summarizer.recentCrawlSummary()

}
