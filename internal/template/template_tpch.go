package template

type Customer struct {
	CCustkey    int64   `json:"c_custkey,omitempty" bson:"c_custkey" dynamodbav:"c_custkey" parquet:"name=c_custkey, type=INT64"`
	CAcctbal    float64 `json:"c_acctbal,omitempty" bson:"c_acctbal" dynamodbav:"c_acctbal" parquet:"name=c_acctbal, type=DOUBLE"`
	CAddress    string  `json:"c_address,omitempty" bson:"c_address" dynamodbav:"c_address" parquet:"name=c_address, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CComment    string  `json:"c_comment,omitempty" bson:"c_comment" dynamodbav:"c_comment" parquet:"name=c_comment, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CName       string  `json:"c_name,omitempty" bson:"c_name" dynamodbav:"c_name" parquet:"name=c_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CMktsegment string  `json:"c_mktsegment,omitempty" bson:"c_mktsegment" dynamodbav:"c_mktsegment" parquet:"name=c_mktsegment, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CNationkey  int64   `json:"c_nationkey,omitempty" bson:"c_nationkey" dynamodbav:"c_nationkey" parquet:"name=c_nationkey, type=INT64"`
	CPhone      string  `json:"c_phone,omitempty" bson:"c_phone" dynamodbav:"c_phone" parquet:"name=c_phone, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

type LineItems struct {
	LSno           int64   `json:"l_sno,omitempty" bson:"l_sno" dynamodbav:"l_sno" parquet:"name=l_sno, type=INT64"`
	LCommitdate    string  `json:"l_commitdate,omitempty" bson:"l_commitdate" dynamodbav:"l_commitdate" parquet:"name=l_commitdate, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LComment       string  `json:"l_comment,omitempty" bson:"l_comment" dynamodbav:"l_comment" parquet:"name=l_comment, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LDiscount      float64 `json:"l_discount,omitempty" bson:"l_discount" dynamodbav:"l_discount" parquet:"name=l_discount, type=DOUBLE"`
	LExtendedprice float64 `json:"l_extendedprice,omitempty" bson:"l_extendedprice" dynamodbav:"l_extendedprice" parquet:"name=l_extendedprice, type=DOUBLE"`
	LLinenumber    int64   `json:"l_linenumber,omitempty" bson:"l_linenumber" dynamodbav:"l_linenumber" parquet:"name=l_linenumber, type=INT64"`
	LLinestatus    string  `json:"l_linestatus,omitempty" bson:"l_linestatus" dynamodbav:"l_linestatus" parquet:"name=l_linestatus, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LOrderkey      int64   `json:"l_orderkey,omitempty" bson:"l_orderkey" dynamodbav:"l_orderkey" parquet:"name=l_orderkey, type=INT64"`
	LPartkey       int64   `json:"l_partkey,omitempty" bson:"l_partkey" dynamodbav:"l_partkey" parquet:"name=l_partkey, type=INT64"`
	LQuantity      int64   `json:"l_quantity,omitempty" bson:"l_quantity" dynamodbav:"l_quantity" parquet:"name=l_quantity, type=INT64"`
	LReceiptdate   string  `json:"l_receiptdate,omitempty" bson:"l_receiptdate" dynamodbav:"l_receiptdate" parquet:"name=l_receiptdate, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LReturnflag    string  `json:"l_returnflag,omitempty" bson:"l_returnflag" dynamodbav:"l_returnflag" parquet:"name=l_returnflag, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LShipdate      string  `json:"l_shipdate,omitempty" bson:"l_shipdate" dynamodbav:"l_shipdate" parquet:"name=l_shipdate, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LShipinstruct  string  `json:"l_shipinstruct,omitempty" bson:"l_shipinstruct" dynamodbav:"l_shipinstruct" parquet:"name=l_shipinstruct, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LShipmode      string  `json:"l_shipmode,omitempty" bson:"l_shipmode" dynamodbav:"l_shipmode" parquet:"name=l_shipmode, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LSuppkey       int64   `json:"l_suppkey,omitempty" bson:"l_suppkey" dynamodbav:"l_suppkey" parquet:"name=l_suppkey, type=INT64"`
}

type Nation struct {
	NNationkey int64  `json:"n_nationkey,omitempty" bson:"n_nationkey" dynamodbav:"n_nationkey" parquet:"name=n_nationkey, type=INT64"`
	NComment   string `json:"n_comment,omitempty" bson:"n_comment" dynamodbav:"n_comment" parquet:"name=n_comment, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	NName      string `json:"n_name,omitempty" bson:"n_name" dynamodbav:"n_name" parquet:"name=n_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	NRegionkey int64  `json:"n_regionkey,omitempty" bson:"n_regionkey" dynamodbav:"n_regionkey" parquet:"name=n_regionkey, type=INT64"`
}

type Order struct {
	OOrderkey      int64   `json:"o_orderkey,omitempty" bson:"o_orderkey" dynamodbav:"o_orderkey" parquet:"name=o_orderkey, type=INT64"`
	OClerk         string  `json:"o_clerk,omitempty" bson:"o_clerk" dynamodbav:"o_clerk" parquet:"name=o_clerk, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	OComment       string  `json:"o_comment,omitempty" bson:"o_comment" dynamodbav:"o_comment" parquet:"name=o_comment, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	OCustkey       int64   `json:"o_custkey,omitempty" bson:"o_custkey" dynamodbav:"o_custkey" parquet:"name=o_custkey, type=INT64"`
	OOrderdate     string  `json:"o_orderdate,omitempty" bson:"o_orderdate" dynamodbav:"o_orderdate" parquet:"name=o_orderdate, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	OOrderpriority string  `json:"o_orderpriority,omitempty" bson:"o_orderpriority" dynamodbav:"o_orderpriority" parquet:"name=o_orderpriority, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	OOrderstatus   string  `json:"o_orderstatus,omitempty" bson:"o_orderstatus" dynamodbav:"o_orderstatus" parquet:"name=o_orderstatus, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	OShippriority  int64   `json:"o_shippriority,omitempty" bson:"o_shippriority" dynamodbav:"o_shippriority" parquet:"name=o_shippriority, type=INT64"`
	OTotalprice    float64 `json:"o_totalprice,omitempty" bson:"o_totalprice" dynamodbav:"o_totalprice" parquet:"name=o_totalprice, type=DOUBLE"`
}

type Part struct {
	PPartkey     int64   `json:"p_partkey,omitempty" bson:"p_partkey" dynamodbav:"p_partkey" parquet:"name=p_partkey, type=INT64"`
	PBrand       string  `json:"p_brand,omitempty" bson:"p_brand" dynamodbav:"p_brand" parquet:"name=p_brand, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PComment     string  `json:"p_comment,omitempty" bson:"p_comment" dynamodbav:"p_comment" parquet:"name=p_comment, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PContainer   string  `json:"p_container,omitempty" bson:"p_container" dynamodbav:"p_container" parquet:"name=p_container, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PMfgr        string  `json:"p_mfgr,omitempty" bson:"p_mfgr" dynamodbav:"p_mfgr" parquet:"name=p_mfgr, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PName        string  `json:"p_name,omitempty" bson:"p_name" dynamodbav:"p_name" parquet:"name=p_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PRetailprice float64 `json:"p_retailprice,omitempty" bson:"p_retailprice" dynamodbav:"p_retailprice" parquet:"name=p_retailprice, type=DOUBLE"`
	PSize        int64   `json:"p_size,omitempty" bson:"p_size" dynamodbav:"p_size" parquet:"name=p_size, type=INT64"`
	PType        string  `json:"p_type,omitempty" bson:"p_type" dynamodbav:"p_type" parquet:"name=p_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

type PartSupplier struct {
	PsSno        int64   `json:"ps_sno,omitempty" bson:"ps_sno" dynamodbav:"ps_sno" parquet:"name=ps_sno, type=INT64"`
	PsAvailqty   int64   `json:"ps_availqty,omitempty" bson:"ps_availqty" dynamodbav:"ps_availqty" parquet:"name=ps_availqty, type=INT64"`
	PsComment    string  `json:"ps_comment,omitempty" bson:"ps_comment" dynamodbav:"ps_comment" parquet:"name=ps_comment, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PsPartkey    int64   `json:"ps_partkey,omitempty" bson:"ps_partkey" dynamodbav:"ps_partkey" parquet:"name=ps_partkey, type=INT64"`
	PsSuppkey    int64   `json:"ps_suppkey,omitempty" bson:"ps_suppkey" dynamodbav:"ps_suppkey" parquet:"name=ps_suppkey, type=INT64"`
	PsSupplycost float64 `json:"ps_supplycost,omitempty" bson:"ps_supplycost" dynamodbav:"ps_supplycost" parquet:"name=ps_supplycost, type=DOUBLE"`
}

type Region struct {
	RRegionkey int64  `json:"r_regionkey,omitempty" bson:"r_regionkey" dynamodbav:"r_regionkey" parquet:"name=r_regionkey, type=INT64"`
	RComment   string `json:"r_comment,omitempty" bson:"r_comment" dynamodbav:"r_comment" parquet:"name=r_comment, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	RName      string `json:"r_name,omitempty" bson:"r_name" dynamodbav:"r_name" parquet:"name=r_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

type Supplier struct {
	SSuppkey   int64   `json:"s_suppkey,omitempty" bson:"s_suppkey" dynamodbav:"s_suppkey" parquet:"name=s_suppkey, type=INT64"`
	SAcctbal   float64 `json:"s_acctbal,omitempty" bson:"s_acctbal" dynamodbav:"s_acctbal" parquet:"name=s_acctbal, type=DOUBLE"`
	SAddress   string  `json:"s_address,omitempty" bson:"s_address" dynamodbav:"s_address" parquet:"name=s_address, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SComment   string  `json:"s_comment,omitempty" bson:"s_comment" dynamodbav:"s_comment" parquet:"name=s_comment, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SName      string  `json:"s_name,omitempty" bson:"s_name" dynamodbav:"s_name" parquet:"name=s_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SNationkey int64   `json:"s_nationkey,omitempty" bson:"s_nationkey" dynamodbav:"s_nationkey" parquet:"name=s_nationkey, type=INT64"`
	SPhone     string  `json:"s_phone,omitempty" bson:"s_phone" dynamodbav:"s_phone" parquet:"name=s_phone, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}
