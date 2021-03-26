package de.hpi.tfm.data.socrata.crawl

import de.hpi.tfm.util.CrawlSummarizer

object CrawlSummary extends App {

  val summarizer = new CrawlSummarizer(args(0))
  summarizer.allTimeChangeSummary()
}
