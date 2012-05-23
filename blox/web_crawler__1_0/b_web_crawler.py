from block import *
from logging import ERROR, WARN, INFO, DEBUG
import urllib2
import urlparse
import os
import collections
import time
import tempfile
import shutil

from BeautifulSoup import BeautifulSoup
from collections import defaultdict

class web_crawler(Block):
  def on_load(self, config):
    self.config = config
    self.add_port("input", Port.PUSH, Port.UNNAMED, ["internet_url"])
    self.add_port("input_query", Port.QUERY, Port.UNNAMED, ["internet_url"])
    self.add_port("output", Port.PUSH, Port.UNNAMED, ["internet_url", "url"])
    self.add_port("can_delete", Port.PUSH, Port.UNNAMED, ["internet_url"])
    #mapping of urls downloaded to the local paths
    self.downloaded_files = defaultdict(list)
    self.file_num = 0
    self.total_download_time = 0
    self.fetch_timeout = 4
    self.download_dir = tempfile.mkdtemp()

  @print_benchmarks
  def on_shutdown(self):
    self.log(INFO, "deleting all temporary downloaded files")
    for url in self.downloaded_files.keys():
      self.delete_url(url)
    shutil.rmtree(self.download_dir)
  
  def add_path(self, url, path):
    self.downloaded_files[url].append(path)
  
  def get_new_file_name(self):
    self.file_num += 1
    return self.id + "_" + self.file_num.__str__()

  @benchmark
  def download_url(self, url):
    req = urllib2.Request(url)
    response = urllib2.urlopen(req, None, self.fetch_timeout)
    new_name = self.get_new_file_name()
    path = os.path.join(self.download_dir, new_name)
    with open(path, 'w') as f:
      f.write(response.read())
    response.close()
    self.add_path(url, path)
    return path
  
  def get_related_urls(self, url, path):
    try:
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
    except Exception as e:
      self.log(WARN, "could not get related links for url %r" % url)
      self.log(WARN, "Exception is %r" % e)
      return []
    
  def delete_url(self, url):
    paths = self.downloaded_files[url]
    for path in paths:
      self.log(INFO, "deleting file: %r" % path)
      os.remove(path)
    self.downloaded_files.pop(url)
  
  def new_log(self):
    log = Log()
    log.append_field("internet_url", [])
    log.append_field("url", [])
    log.append_field("asset_of", [])
    return log
    
  def add_url(self, log, iurl, path, asset_of):
    row = {"internet_url": iurl,
           "url": BlockUtils.generate_url_for_path(path,
                                                   self.ip_address),
           "asset_of": asset_of}
    log.append_row(row)

  def process_download(self, log):
    for url in log.log["internet_url"]:
      log = self.new_log()
      self.log(INFO, "got url: %r" % url)
      try:
        path = self.download_url(url)
        self.add_url(log, url, path, url)
      except Exception as e:
        self.log(WARN, "could not download main url %r" % url)
        self.log(WARN, "Exception is %r" % e)
        return
      related_urls = self.get_related_urls(url, path)
      for i, rurl in enumerate(related_urls):
        try:
          path = self.download_url(rurl)
          self.add_url(log, rurl, path, url)
          #yield after every 2 seconds
          if time.time() - self.last_poll_time > 2:
            yield
        except Exception as e:
          self.log(WARN, "could not download url %r" % rurl)
          self.log(WARN, "Exception is %r" % e)
          continue
      self.push("output", log)
    
  def recv_push(self, port, log):
    if port == "input":
      gen = self.process_download(log)
      if gen != None:
        for g in gen:
          yield
    elif port == "can_delete":
      for url in log.log["internet_url"]:
        self.delete_url(url)

  def recv_query(self, port, log):
    if port == "input_query":
      ret = Log()
      gen = self.process_download(log)
      if gen != None:
        for g in gen:
          yield
        ret.log["result"] = True
      else:
        ret.log["result"] = False
      self.return_query_res(port, ret)
    else:
      self.log(ERROR, "No such port")