#!/bin/bash
# ClickBench Benchmark for Trino-OpenSearch Integration
#
# Runs all 43 ClickBench queries against the real 14GB hits.parquet dataset
# (99,997,497 rows) through the /_plugins/_trino_sql/v1/statement REST API.
#
# This script:
#   1. Starts OpenSearch with the Trino plugin enabled
#   2. Creates a Hive external table pointing to the Parquet file
#   3. Runs all 43 ClickBench queries with timing
#   4. Prints a results table
#
# Prerequisites:
#   - hits.parquet downloaded to trino/benchmark/data/
#     wget -O trino/benchmark/data/hits.parquet \
#       https://datasets.clickhouse.com/hits_compatible/hits.parquet
#   - Plugin built: ./gradlew :opensearch-sql-plugin:bundlePlugin
#
# Usage:
#   ./trino/benchmark/run_clickbench.sh
#
# Environment variables:
#   OPENSEARCH_HEAP  — JVM heap size (default: 16g)
#   TRIES            — runs per query (default: 1)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"
PARQUET_FILE="${DATA_DIR}/hits.parquet"
QUERIES_FILE="${REPO_DIR}/trino/trino-integ-test/src/test/resources/clickbench/queries.sql"
HEAP="${OPENSEARCH_HEAP:-16g}"
TRIES="${TRIES:-1}"

# ---------- Validation ----------
if [ ! -f "${PARQUET_FILE}" ]; then
    echo "ERROR: ${PARQUET_FILE} not found"
    echo "Download: wget -O ${PARQUET_FILE} https://datasets.clickhouse.com/hits_compatible/hits.parquet"
    exit 1
fi

if [ ! -f "${QUERIES_FILE}" ]; then
    echo "ERROR: ${QUERIES_FILE} not found"
    exit 1
fi

echo "=== ClickBench Benchmark for Trino-OpenSearch ==="
echo "Data:    ${PARQUET_FILE} ($(du -h "${PARQUET_FILE}" | cut -f1))"
echo "Heap:    ${HEAP}"
echo "Tries:   ${TRIES}"
echo ""

# ---------- Step 1: Start OpenSearch ----------
echo "Step 1: Starting OpenSearch with Trino plugin..."

# Build if needed
if [ ! -f "${REPO_DIR}/plugin/build/distributions/opensearch-sql-"*.zip ]; then
    echo "  Building plugin..."
    (cd "${REPO_DIR}" && ./gradlew :opensearch-sql-plugin:bundlePlugin -q)
fi

# Find or create test cluster
CLUSTER_DIR="${REPO_DIR}/plugin/build/testclusters/integTest-0"
if [ ! -d "${CLUSTER_DIR}" ]; then
    echo "  Creating test cluster (first run)..."
    (cd "${REPO_DIR}" && ./gradlew :opensearch-sql-plugin:run -DenableTrino &)
    sleep 10
    # Kill it — we just needed it to create the cluster directory
    pkill -f "testclusters/integTest" 2>/dev/null || true
    sleep 3
fi

DISTRO_DIR="${CLUSTER_DIR}/distro/3.6.0-ARCHIVE"
if [ ! -d "${DISTRO_DIR}" ]; then
    echo "ERROR: OpenSearch distribution not found at ${DISTRO_DIR}"
    echo "Run first: ./gradlew :opensearch-sql-plugin:run -DenableTrino (then Ctrl+C)"
    exit 1
fi

# Ensure config has Trino enabled
grep -q "plugins.trino.enabled" "${CLUSTER_DIR}/config/opensearch.yml" || \
    echo "plugins.trino.enabled: true" >> "${CLUSTER_DIR}/config/opensearch.yml"

# Copy latest plugin jar
cp "${REPO_DIR}/trino/trino-opensearch/build/libs/trino-opensearch-"*.jar \
   "${DISTRO_DIR}/plugins/opensearch-sql/" 2>/dev/null || true

# Clean data and start
rm -rf "${CLUSTER_DIR}/data"
export OPENSEARCH_JAVA_OPTS="-Xms${HEAP} -Xmx${HEAP}"

# Kill any existing instance
pkill -f "penghuo.*opensearch.*bootstrap" 2>/dev/null || true
sleep 2

nohup "${DISTRO_DIR}/bin/opensearch" > /tmp/clickbench-os.log 2>&1 &
OS_PID=$!
echo "  OpenSearch PID: ${OS_PID}"

# Discover port
OS_PORT=""
for i in $(seq 1 90); do
    OS_PORT=$(grep "publish_address.*http" /tmp/clickbench-os.log 2>/dev/null | \
              grep -oP '127\.0\.0\.1:\K\d+' | tail -1)
    if [ -n "${OS_PORT}" ]; then
        # Verify Trino is ready
        if curl -s "http://localhost:${OS_PORT}/_plugins/_trino_sql/v1/statement" \
            -X POST -H "Content-Type: application/json" \
            -H "X-Trino-Catalog: tpch" -H "X-Trino-Schema: tiny" \
            -d '{"query":"SELECT 1"}' 2>/dev/null | grep -q "FINISHED"; then
            break
        fi
    fi
    if [ $i -eq 90 ]; then
        echo "ERROR: OpenSearch failed to start. Check /tmp/clickbench-os.log"
        exit 1
    fi
    sleep 2
done

ENDPOINT="http://localhost:${OS_PORT}/_plugins/_trino_sql/v1/statement"
echo "  Endpoint: ${ENDPOINT}"
echo ""

# ---------- Helper: run SQL ----------
run_sql() {
    local sql="$1"
    local catalog="${2:-hive}"
    local schema="${3:-bench2}"
    python3 -c "
import json, urllib.request, sys
sql = sys.argv[1]
body = json.dumps({'query': sql}).encode()
req = urllib.request.Request('${ENDPOINT}', data=body,
    headers={'Content-Type': 'application/json',
             'X-Trino-Catalog': '${2:-hive}', 'X-Trino-Schema': '${3:-bench2}'})
resp = urllib.request.urlopen(req, timeout=3600).read().decode()
print(resp)
" "$sql"
}

# ---------- Step 2: Create Hive table ----------
echo "Step 2: Creating Hive external table on Parquet file..."

run_sql "CREATE SCHEMA IF NOT EXISTS hive.bench2 WITH (location = 'file://${DATA_DIR}')" "hive" "default" > /dev/null

COLS="watchid BIGINT, javaenable SMALLINT, title VARCHAR, goodevent SMALLINT, eventtime BIGINT, eventdate INTEGER, counterid INTEGER, clientip INTEGER, regionid INTEGER, userid BIGINT, counterclass SMALLINT, os SMALLINT, useragent SMALLINT, url VARCHAR, referer VARCHAR, isrefresh SMALLINT, referercategoryid SMALLINT, refererregionid INTEGER, urlcategoryid SMALLINT, urlregionid INTEGER, resolutionwidth SMALLINT, resolutionheight SMALLINT, resolutiondepth SMALLINT, flashmajor SMALLINT, flashminor SMALLINT, flashminor2 VARCHAR, netmajor SMALLINT, netminor SMALLINT, useragentmajor SMALLINT, useragentminor VARCHAR, cookieenable SMALLINT, javascriptenable SMALLINT, ismobile SMALLINT, mobilephone SMALLINT, mobilephonemodel VARCHAR, params VARCHAR, ipnetworkid INTEGER, traficsourceid SMALLINT, searchengineid SMALLINT, searchphrase VARCHAR, advengineid SMALLINT, isartifical SMALLINT, windowclientwidth SMALLINT, windowclientheight SMALLINT, clienttimezone SMALLINT, clienteventtime BIGINT, silverlightversion1 SMALLINT, silverlightversion2 SMALLINT, silverlightversion3 INTEGER, silverlightversion4 SMALLINT, pagecharset VARCHAR, codeversion INTEGER, islink SMALLINT, isdownload SMALLINT, isnotbounce SMALLINT, funiqid BIGINT, originalurl VARCHAR, hid INTEGER, isoldcounter SMALLINT, isevent SMALLINT, isparameter SMALLINT, dontcounthits SMALLINT, withhash SMALLINT, hitcolor VARCHAR, localeventtime BIGINT, age SMALLINT, sex SMALLINT, income SMALLINT, interests SMALLINT, robotness SMALLINT, remoteip INTEGER, windowname INTEGER, openername INTEGER, historylength SMALLINT, browserlanguage VARCHAR, browsercountry VARCHAR, socialnetwork VARCHAR, socialaction VARCHAR, httperror SMALLINT, sendtiming INTEGER, dnstiming INTEGER, connecttiming INTEGER, responsestarttiming INTEGER, responseendtiming INTEGER, fetchtiming INTEGER, socialsourcenetworkid SMALLINT, socialsourcepage VARCHAR, paramprice BIGINT, paramorderid VARCHAR, paramcurrency VARCHAR, paramcurrencyid SMALLINT, openstatservicename VARCHAR, openstatcampaignid VARCHAR, openstatadid VARCHAR, openstatsourceid VARCHAR, utmsource VARCHAR, utmmedium VARCHAR, utmcampaign VARCHAR, utmcontent VARCHAR, utmterm VARCHAR, fromtag VARCHAR, hasgclid SMALLINT, refererhash BIGINT, urlhash BIGINT, clid INTEGER"

run_sql "CREATE TABLE IF NOT EXISTS hive.bench2.hits (${COLS}) WITH (external_location = 'file://${DATA_DIR}', format = 'PARQUET')" "hive" "bench2" > /dev/null

# Verify row count
COUNT=$(run_sql "SELECT COUNT(*) FROM hive.bench2.hits" "hive" "bench2" | python3 -c "import json,sys; print(json.load(sys.stdin)['data'][0][0])")
echo "  Table: hive.bench2.hits (${COUNT} rows)"
echo ""

# ---------- Step 3: Run queries ----------
echo "Step 3: Running 43 ClickBench queries..."
echo ""

python3 << 'PYEOF'
import json, urllib.request, time, re, sys

endpoint = "${ENDPOINT}"
tries = ${TRIES}

queries_raw = open("${QUERIES_FILE}").readlines()
queries = [q.strip().rstrip(';') for q in queries_raw if q.strip() and not q.strip().startswith('--')]

# Column name mapping (Parquet uses CamelCase, Hive lowercases)
col_map = {'WatchID':'watchid','JavaEnable':'javaenable','Title':'title','GoodEvent':'goodevent','EventTime':'eventtime','EventDate':'eventdate','CounterID':'counterid','ClientIP':'clientip','RegionID':'regionid','UserID':'userid','CounterClass':'counterclass','OS':'os','UserAgent':'useragent','URL':'url','Referer':'referer','IsRefresh':'isrefresh','RefererCategoryID':'referercategoryid','RefererRegionID':'refererregionid','URLCategoryID':'urlcategoryid','URLRegionID':'urlregionid','ResolutionWidth':'resolutionwidth','ResolutionHeight':'resolutionheight','ResolutionDepth':'resolutiondepth','FlashMajor':'flashmajor','FlashMinor':'flashminor','FlashMinor2':'flashminor2','NetMajor':'netmajor','NetMinor':'netminor','UserAgentMajor':'useragentmajor','UserAgentMinor':'useragentminor','CookieEnable':'cookieenable','JavascriptEnable':'javascriptenable','IsMobile':'ismobile','MobilePhone':'mobilephone','MobilePhoneModel':'mobilephonemodel','Params':'params','IPNetworkID':'ipnetworkid','TraficSourceID':'traficsourceid','SearchEngineID':'searchengineid','SearchPhrase':'searchphrase','AdvEngineID':'advengineid','IsArtifical':'isartifical','WindowClientWidth':'windowclientwidth','WindowClientHeight':'windowclientheight','ClientTimeZone':'clienttimezone','ClientEventTime':'clienteventtime','SilverlightVersion1':'silverlightversion1','SilverlightVersion2':'silverlightversion2','SilverlightVersion3':'silverlightversion3','SilverlightVersion4':'silverlightversion4','PageCharset':'pagecharset','CodeVersion':'codeversion','IsLink':'islink','IsDownload':'isdownload','IsNotBounce':'isnotbounce','FUniqID':'funiqid','OriginalURL':'originalurl','HID':'hid','IsOldCounter':'isoldcounter','IsEvent':'isevent','IsParameter':'isparameter','DontCountHits':'dontcounthits','WithHash':'withhash','HitColor':'hitcolor','LocalEventTime':'localeventtime','Age':'age','Sex':'sex','Income':'income','Interests':'interests','Robotness':'robotness','RemoteIP':'remoteip','WindowName':'windowname','OpenerName':'openername','HistoryLength':'historylength','BrowserLanguage':'browserlanguage','BrowserCountry':'browsercountry','SocialNetwork':'socialnetwork','SocialAction':'socialaction','HTTPError':'httperror','SendTiming':'sendtiming','DNSTiming':'dnstiming','ConnectTiming':'connecttiming','ResponseStartTiming':'responsestarttiming','ResponseEndTiming':'responseendtiming','FetchTiming':'fetchtiming','SocialSourceNetworkID':'socialsourcenetworkid','SocialSourcePage':'socialsourcepage','ParamPrice':'paramprice','ParamOrderID':'paramorderid','ParamCurrency':'paramcurrency','ParamCurrencyID':'paramcurrencyid','OpenstatServiceName':'openstatservicename','OpenstatCampaignID':'openstatcampaignid','OpenstatAdID':'openstatadid','OpenstatSourceID':'openstatsourceid','UTMSource':'utmsource','UTMMedium':'utmmedium','UTMCampaign':'utmcampaign','UTMContent':'utmcontent','UTMTerm':'utmterm','FromTag':'fromtag','HasGCLID':'hasgclid','RefererHash':'refererhash','URLHash':'urlhash','CLID':'clid','PageViews':'pageviews','Src':'src','Dst':'dst'}

def lc(sql):
    for c, l in sorted(col_map.items(), key=lambda x: -len(x[0])):
        sql = re.sub(r'\b' + c + r'\b', l, sql)
    return sql

def adapt(sql):
    sql = lc(sql)
    sql = sql.replace("DATE '2013-07-01'","15887").replace("DATE '2013-07-14'","15900")
    sql = sql.replace("DATE '2013-07-15'","15901").replace("DATE '2013-07-31'","15917")
    sql = sql.replace("extract(minute FROM eventtime)","CAST((eventtime % 3600) / 60 AS INTEGER)")
    sql = sql.replace("DATE_TRUNC('minute', eventtime)","(eventtime / 60) * 60")
    return sql

def run_sql(sql, timeout=600):
    body = json.dumps({"query": sql}).encode()
    req = urllib.request.Request(endpoint, data=body,
        headers={"Content-Type": "application/json",
                 "X-Trino-Catalog": "hive", "X-Trino-Schema": "bench2"})
    resp = urllib.request.urlopen(req, timeout=timeout).read().decode()
    return json.loads(resp)

# Header
print(f"{'Query':<7} ", end="")
for t in range(1, tries+1):
    print(f"{'Try'+str(t):>10}", end="")
if tries > 1:
    print(f"  {'Min':>8} {'Med':>8} {'Max':>8}", end="")
print(f"  {'Status':<6}")
print("-" * (7 + tries*10 + (24 if tries > 1 else 0) + 8))

passed = failed = 0
total = 0
results = []

for i, raw in enumerate(queries):
    sql = adapt(raw)
    times = []
    ok = True
    err_msg = ""

    for t in range(tries):
        s = time.time()
        try:
            r = run_sql(sql, timeout=600)
            e = time.time() - s
            if r.get("stats",{}).get("state") == "FINISHED":
                times.append(e)
            else:
                ok = False
                err_msg = r.get("error",{}).get("message","?")[:40]
                times.append(-1)
        except Exception as ex:
            ok = False
            err_msg = str(ex)[:40]
            times.append(-1)

    print(f"Q{i:<5} ", end="")
    for t_val in times:
        if t_val >= 0:
            print(f"{t_val:>9.1f}s", end="")
        else:
            print(f"{'FAIL':>10}", end="")

    if ok:
        sorted_t = sorted(times)
        mn, med, mx = sorted_t[0], sorted_t[len(sorted_t)//2], sorted_t[-1]
        if tries > 1:
            print(f"  {mn:>7.1f}s {med:>7.1f}s {mx:>7.1f}s", end="")
        print(f"  OK")
        passed += 1
        total += (mn if tries > 1 else times[0])
        results.append((i, mn if tries > 1 else times[0], "OK"))
    else:
        if tries > 1:
            print(f"  {'':>8} {'':>8} {'':>8}", end="")
        print(f"  FAIL ({err_msg})")
        failed += 1
        results.append((i, -1, "FAIL"))

print("-" * (7 + tries*10 + (24 if tries > 1 else 0) + 8))
print(f"\nPassed: {passed}/43, Failed: {failed}/43")
print(f"Total time: {total:.0f}s (best of {tries})" if tries > 1 else f"Total time: {total:.0f}s")
print(f"Average: {total/max(passed,1):.1f}s per query")
PYEOF

# ---------- Step 4: Cleanup ----------
echo ""
echo "Stopping OpenSearch (PID: ${OS_PID})..."
kill ${OS_PID} 2>/dev/null || true
echo "Done."
