from block import *
from logging import ERROR, WARN, INFO, DEBUG
import urllib2
import urlparse
import os
import collections
import time

from BeautifulSoup import BeautifulSoup
from collections import defaultdict

class web_crawler(Block):
  def on_load(self, config):
    self.config = config
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["internet_url"])
    self.add_port("rpc", Port.QUERY, Port.UNNAMED, ["internet_url"])
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["internet_url", "url"])
    self.add_port("can_delete", Port.PUSH, Port.UNNAMED, ["internet_url"])
    #mapping of urls downloaded to the local paths
    self.downloaded_files = defaultdict(list)
    self.file_num = 0
    self.total_download_time = 0

  def recv_push(self, port, log):
    if port == "input":
      self.process_crawl(log)
    elif port == "can_delete":
      for url in log.log["internet_url"]:
        self.delete_url(url)
  
  def on_shutdown(self):
    self.log(INFO, "deleting all temporary downloaded files")
    for url in self.downloaded_files.keys():
      self.delete_url(url)
  
  def add_path(self, url, path):
    self.downloaded_files[url].append(path)
  
  def get_new_file_name(self):
    self.file_num += 1
    return self.id + "_" + self.file_num.__str__()
    
  def download_url(self, url):
    start = time.time()
    req = urllib2.Request(url)
    response = urllib2.urlopen(req)
    new_name = self.get_new_file_name()
    with open(new_name, 'w') as f:
      f.write(response.read())
    path = os.path.join(os.getcwd(), new_name)
    duration = time.time() - start
    self.total_download_time += duration
    self.log(INFO, "perf: time for url: %r is %r" % (url, duration))
    self.log(INFO, "perf: total download time so far is %r" % (self.total_download_time))
    return path
  
  def get_related_urls(self, url, path):
    with open(path, 'r') as f:
      soup = BeautifulSoup(f.read())
    img_links = [l.get('src') for l in soup.findAll('img')]
    css_links = [l.get('href') for l in soup.findAll('link') if l.has_key('rel') and l['rel'].lower() == 'stylesheet']
    #extract links with valid sources
    links = [l for l in (img_links + css_links) if l != None]
    #convert relative links into absolute
    #TODO: does not work in all cases
    absolute_links = [urlparse.urljoin(url, l) if l[:4] != 'http' else l for l in links]
    self.log(INFO, "absolute_links: %r" % absolute_links)
    return absolute_links
    
  def download_all_urls(self, url):
    self.log(INFO, "got url: %r" % url)
    try:
      path = self.download_url(url)
    except Exception as e:
      self.log(WARN, "could not download url %r" % url)
      self.log(WARN, "Exception is %r" % e)
      return [], []
    self.add_path(url, path)
    internet_urls = [url]
    local_urls = [BlockUtils.generate_url_for_path(path)]
    related_urls = self.get_related_urls(url, path)
    working_related_urls = []
    for rurl in related_urls:
      try:
        path = self.download_url(rurl)
        self.add_path(rurl, path)
        local_urls.append(BlockUtils.generate_url_for_path(path))
        working_related_urls.append(rurl)
      except Exception as e:
        self.log(WARN, "could not download url %r" % rurl)
        self.log(WARN, "Exception is %r" % e)
        continue
    internet_urls.extend(working_related_urls)
    # self.log(INFO, "internet_urls %r" % internet_urls)
    return internet_urls, local_urls
    
  def process_crawl(self, log):
    asset_of = []
    internet_urls, local_urls = [], []
    for url in log.log["internet_url"]:
      i, l = self.download_all_urls(url)
      if i == []:
        return False
      related_this = [url for u in i]
      internet_urls.extend(i)
      local_urls.extend(l)
      asset_of.extend(related_this)
    log = Log()
    log.append_field("internet_url", internet_urls)
    log.append_field("url", local_urls)
    log.append_field("asset_of", asset_of)
    self.push("output", log)
    return True
    
  def delete_url(self, url):
    paths = self.downloaded_files[url]
    for path in paths:
      self.log(INFO, "deleting file: %r" % path)
      os.remove(path)
    self.downloaded_files.pop(url)
  
  def recv_query(self, port, log):
    res = self.process_crawl(log)
    ret = Log()
    ret.log["result"] = res
    self.return_query_res(port, ret)