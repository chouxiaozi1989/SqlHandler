package org.caiyi.SqlHandler.core;

import org.caiyi.SqlHandler.util.DateUtil;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.InvalidResultSetAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;

import javax.sql.DataSource;
import java.io.Serializable;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class Sql implements Serializable {
    private static final boolean SQL_QUERY_size_LIMIT = true;
    private static final long serialVersionUID = 1L;
    private static final int MAX_ROW_CAPACITY = 3000;
    private static final int JDBC_FETCH_SIZE = 1000;
    private String sqlString = null;
    private ArrayList<Object> para = null;
    private final JdbcTemplate jdbcTemplate;
    private ArrayList<Object[]> batchParaList = null;
    private boolean batchFlag = false;
    private int paraCount = 0;

    public Sql(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    private String trim(String s) {
        if (s == null) {
            return null;
        } else {
            s = s.trim();
            return s;
        }
    }

    /**
     * 设置、获取Sql语句
     */
    public void setSql(String sqlstmt) throws Exception {
        if (batchFlag) {
            throw new Exception(
                    "当前SQL类已经设置了Batch参数，不能重新设置SQL String，请重新实例化SQL或执行ExecuteBatch或执行resetBatch方法后，再设置SQL String。");
        }
        this.sqlString = sqlstmt;

        // 根据sqlstmt中参数个数来初始化参数数组
        para = new ArrayList<Object>();
        // 清空batch参数列表
        batchParaList = null;
        paraCount = 0;
    }

    public final String getSql() {
        return this.sqlString;
    }

    /**
     * 自定义数据类型
     */
    private class NullValue {
        private int type;

        public NullValue(int type) {
            this.type = type;
        }

        public int getType() {
            return this.type;
        }
    }


    private static class BlobValue implements Serializable {
        private static final long serialVersionUID = 4987534051624333379L;
        private byte[] value;

        public BlobValue(byte[] value) {
            if (value == null) {
                value = new byte[]{};
            }
            this.value = value;
        }

        public BlobValue(String value) {
            if (value == null) {
                value = "";
            }
            try {
                this.value = value.getBytes("GBK");
            } catch (Exception e) {
                this.value = value.getBytes();
            }
        }

        public byte[] getValue() {
            return this.value;
        }

        public int getLength() {
            return this.value.length;
        }
    }


    private static class ClobValue implements Serializable {
        private static final long serialVersionUID = -2900880620133106928L;
        private final String value;

        public ClobValue(String value) {
            this.value = value == null ? "" : value;
        }

        public String getValue() {
            return this.value;
        }

        public int getLength() {
            return this.value.length();
        }
    }


    /**
     * 设置各种类型的SQL参数
     */
    private void handlePara(int index, Object value) throws Exception {
        if (index > para.size()) {
            para.add(index - 1, value);
        } else {
            para.set(index - 1, value);
        }
    }

    public void setString(int index, String value) throws Exception {
        if (value == null) {
            this.setNull(index, Types.VARCHAR);
        } else {
            handlePara(index, value);
        }
    }

    public void setBigDecimal(int index, BigDecimal value) throws Exception {
        if (value == null) {
            this.setNull(index, Types.VARCHAR);
        } else {
            handlePara(index, value);
        }
    }

    public void setBigDecimal(int index, String value) throws Exception {
        if (value == null || "".equalsIgnoreCase(trim(value))) {
            this.setNull(index, Types.VARCHAR);
        } else {
            if (value.matches("^\\d+(\\.\\d+)?$")) {
                handlePara(index, new BigDecimal(value));
            } else {
                throw new Exception("Sql.setBigDecimal(int index, String value)的入参【value】当前为【" + value + "】,不合法,请检查！");
            }
        }
    }

    public void setBigDecimal(int index, int value) throws Exception {
        handlePara(index, new BigDecimal(value));
    }

    public void setBigDecimal(int index, Integer value) throws Exception {
        if (value == null) {
            this.setNull(index, Types.VARCHAR);
        } else {
            handlePara(index, new BigDecimal(value));
        }
    }

    public void setBigDecimal(int index, double value) throws Exception {
        handlePara(index, new BigDecimal(Double.toString(value)));
    }

    public void setBigDeicmal(int index, Double value) throws Exception {
        if (value == null) {
            handlePara(index, Types.VARCHAR);
        } else {
            handlePara(index, new BigDecimal(Double.toString(value)));
        }
    }

    public void setInt(int index, int value) throws Exception {
        handlePara(index, value);
    }

    public void setInt(int index, Integer value) throws Exception {
        if (value == null) {
            this.setNull(index, Types.VARCHAR);
        } else {
            handlePara(index, value);
        }
    }

    public void setClob(int index, String value) throws Exception {
        if (value == null) {
            this.setNull(index, Types.LONGVARCHAR);
        } else {
            handlePara(index, new ClobValue(value));
        }
    }

    public void setBlob(int index, String value) throws Exception {
        if (value == null) {
            this.setNull(index, Types.BINARY);
        } else {
            handlePara(index, new BlobValue(value));
        }
    }

    public void setBlob(int index, byte[] value) throws Exception {
        if (value == null) {
            this.setNull(index, Types.BINARY);
        } else {
            handlePara(index, new BlobValue(value));
        }
    }

    public void setDouble(int index, double value) throws Exception {
        handlePara(index, value);
    }

    public void setDouble(int index, Double value) throws Exception {
        if (value == null) {
            this.setNull(index, Types.VARCHAR);
        } else {
            handlePara(index, value);
        }
    }

    public void setBoolean(int index, boolean value) throws Exception {
        handlePara(index, Boolean.valueOf(value));
    }

    public void setDate(int index, java.util.Date value) throws Exception {
        if (value == null) {
            this.setNull(index, Types.DATE);
        } else {
            handlePara(index, new java.sql.Date(((java.util.Date) value).getTime()));
        }
    }

    public void setDateTime(int index, java.util.Date value) throws Exception {
        if (value == null) {
            this.setNull(index, Types.DATE);
        } else {
            handlePara(index, new Timestamp(((java.util.Date) value).getTime()));
        }
    }

    public void setTimestamp(int index, Timestamp value) throws Exception {
        if (value == null) {
            this.setNull(index, Types.TIMESTAMP);
        } else {
            handlePara(index, value);
        }
    }

    public void setLongVarchar(int index, String value) throws Exception {
        if (value == null) {
            this.setNull(index, Types.LONGVARCHAR);
        } else {
            handlePara(index, new StringBuffer(value));
        }
    }

    public void setNull(int index, int sqlType) throws Exception {
        handlePara(index, new NullValue(sqlType));
    }

    private void FeBeCheck() throws Exception {
        if (isSqlContainsUpdate()) {
            throw new Exception("FE端查询语句不能包含【for update】语句!");
        }
    }

    /**
     * 检查SQL中是否包含“for update”语句
     * <p>
     * 注：BE端执行的查询语句，不能有for update
     */
    private boolean isSqlContainsUpdate() {
        if (this.sqlString == null) {
            return false;
        }

        return this.sqlString.contains("for update");
    }

    /**
     * 执行查询语句
     */
    public ArrayList<HashMap<String, Object>> executeQuery() throws Exception {
        ArrayList<HashMap<String, Object>> ds = null;
        try {
            if (batchFlag) {
                throw new Exception("当前SQL类已经设置了Batch参数，不能执行executeQuery，请重新实例化SQL或执行ExecuteBatch或执行resetBatch方法。");
            }
            ds = executeSelectSQL(this.sqlString, this.para);
            //检验获取数据是否超过预设最大数据行数
            if (SQL_QUERY_size_LIMIT) {
                if (ds.size() > MAX_ROW_CAPACITY) {
                    throw new Exception("【SQL获取的数据超过" + MAX_ROW_CAPACITY + "，总行数为["
                            + ds.size() + "]】SQL:【" + getSql() + "】.");
                }
            }
            return ds;
        } catch (Exception ex) {
            throw new Exception(ex);
        }
    }

    /**
     * 忽略最大行数限制，执行查询语句
     */
    public ArrayList<HashMap<String, Object>> executeQueryWithoutLimit() throws Exception {
        ArrayList<HashMap<String, Object>> ds = null;
        try {
            if (batchFlag) {
                throw new Exception("当前SQL类已经设置了Batch参数，不能执行executeQuery，请重新实例化SQL或执行ExecuteBatch或执行resetBatch方法。");
            }
            ds = executeSelectSQL(this.sqlString, this.para);

            return ds;
        } catch (Exception ex) {
            throw new Exception(ex);
        }
    }

    public ArrayList<HashMap<String, Object>> executeQuery(int minNum, int maxNum) throws Exception {
        ArrayList<HashMap<String, Object>> ds = this.executeQuery();
        if (ds.size() < minNum || ds.size() > maxNum) {
            throw new Exception("查询结果的记录数目[" + ds.size() + "]不在指定范围["
                    + String.valueOf(minNum) + "-" + String.valueOf(maxNum)
                    + "]内！");
        }
        return ds;
    }

    public ArrayList<HashMap<String, Object>> executeQuery(int minNum) throws Exception {
        ArrayList<HashMap<String, Object>> ds = this.executeQuery();
        if (ds.size() < minNum) {
            throw new Exception("查询结果的记录数目[" + ds.size() + "]不在指定范围["
                    + String.valueOf(minNum) + "-1000000....]内！");
        }
        return ds;
    }


    /**
     * 执行更新语句
     */
    public int executeUpdate() throws Exception {
        int vi = 0;
        try {
            if (batchFlag) {
                throw new Exception(
                        "当前SQL类已经设置了Batch参数，不能执行executeUpdate，请重新实例化SQL或执行ExecuteBatch或执行resetBatch方法。");
            }
            final Object[] exePara = new Object[this.para.size()];
            for (int i = 0; i < this.para.size(); i++) {
                exePara[i] = this.para.get(i);
            }

            PreparedStatementSetter pss = new PreparedStatementSetter() {
                public void setValues(PreparedStatement pstmt)
                        throws SQLException {
                    try {
                        setParas(pstmt, (Object[]) exePara);
                    } catch (Exception e) {
                        throw new SQLException();
                    }
                }
            };
            vi = jdbcTemplate.update(this.sqlString, pss);

            return vi;
        } catch (Exception ex) {
            throw new Exception(ex);
        }
    }

    public int executeUpdate(int minNum, int maxNum) throws Exception {
        int vi = this.executeUpdate();
        if (vi < minNum || vi > maxNum) {
            throw new Exception("数操作影响的的记录数目[" + vi + "]不在指定范围["
                    + String.valueOf(minNum) + "-" + String.valueOf(maxNum)
                    + "]内！");
        }
        return vi;
    }

    public int executeUpdate(int minNum) throws Exception {
        int vi = this.executeUpdate();
        if (vi < minNum) {
            throw new Exception("数据操作影响的的记录数目[" + vi + "]不在指定范围["
                    + String.valueOf(minNum) + "-100000......]内！");
        }
        return vi;
    }


    /**
     * 批设置相关逻辑
     */
    public void addBatch() throws Exception {
        if (batchParaList == null) {
            batchParaList = new ArrayList<Object[]>();
        }
        if (para == null) {
            return;
        }
        batchParaList.add(para.toArray());
        para = new ArrayList<Object>();
        batchFlag = true;
    }

    public void resetBatch() {
        batchParaList = null;
        batchFlag = false;
    }

    public int[] executeBatch() throws Exception {
        if (batchParaList == null) {
            return null;
        }
        try {
            BatchPreparedStatementSetter setter = new BatchPreparedStatementSetter() {
                public void setValues(PreparedStatement pstmt, int i)
                        throws SQLException {
                    Object[] paras = (Object[]) batchParaList.get(i);
                    try {
                        setParas(pstmt, paras);
                    } catch (Exception e) {
                        throw new SQLException();
                    }
                }

                public int getBatchSize() {
                    return batchParaList.size();
                }
            };
            int[] result = jdbcTemplate.batchUpdate(this.sqlString, setter);
            // 清空batch参数列表
            batchParaList = null;

            int _sum = 0;
            for (int i : result) {
                _sum += i;
            }
            return result;
        } catch (Exception ex) {
            throw new Exception(ex);
        } finally {
            // 处理批量处理标志为false
            batchFlag = false;
        }
    }

    private void setParas(PreparedStatement pstmt, Object[] para)
            throws Exception {
        if (para == null) {
            return;
        }
        try {
            for (int i = 0; i < para.length; i++) {
                Object o = para[i];
                if (o instanceof java.lang.Integer) {
                    pstmt.setInt(i + 1, (Integer) o);
                } else if (o instanceof java.lang.Double) {
                    pstmt.setDouble(i + 1, (Double) o);
                } else if (o instanceof java.lang.Boolean) {
                    pstmt.setBoolean(i + 1, (Boolean) o);
                } else if (o instanceof java.lang.String) {
                    pstmt.setString(i + 1, (String) o);
                } else if (o instanceof java.sql.Date) {
                    pstmt.setDate(i + 1, (java.sql.Date) o);
                } else if (o instanceof java.sql.Timestamp) {
                    pstmt.setTimestamp(i + 1, (java.sql.Timestamp) o);
                } else if (o instanceof java.util.Date) {
                    pstmt.setDate(i + 1, new java.sql.Date(((java.util.Date) o).getTime()));
                } else if (o instanceof java.sql.Blob) {
                    pstmt.setBlob(i + 1, (java.sql.Blob) o);
                } else if (o instanceof BlobValue) {
                    BlobValue blobValue = (BlobValue) o;
                    pstmt.setBinaryStream(i + 1, new java.io.ByteArrayInputStream(blobValue.getValue()), blobValue.getLength());
                } else if (o instanceof ClobValue) {
                    ClobValue clobValue = (ClobValue) o;
                    StringReader r = new StringReader(clobValue.getValue());
                    pstmt.setCharacterStream(i + 1, r, clobValue.getLength());
                } else if (o instanceof StringBuffer) {
                    StringBuffer longVarCharValue = (StringBuffer) o;
                    StringReader reader = new StringReader(longVarCharValue.toString());
                    pstmt.setCharacterStream(i + 1, reader, longVarCharValue.toString().length());
                } else if (o instanceof NullValue) {
                    pstmt.setNull(i + 1, ((NullValue) o).getType());
                } else if (o instanceof BigDecimal) {
                    pstmt.setBigDecimal(i + 1, (BigDecimal) o);
                } else if (o == null) {
                    throw new Exception("第" + (i + 1) + "个参数未定义");
                } else {
                    throw new Exception("第" + (i + 1) + "个参数类型不合法");
                }
            }
        } catch (SQLException e) {
            throw new SQLException();
        }
    }

    private LinkedHashMap<String, String> generateTypeListFromResultSetMetadata(
            ResultSetMetaData rsmd) throws SQLException {
        String[] column = new String[rsmd.getColumnCount()];
        LinkedHashMap<String, String> typeList = new LinkedHashMap<String, String>();
        for (int i = 0; i < column.length; i++) {
            column[i] = rsmd.getColumnName(i + 1);
            column[i] = column[i].toLowerCase();
            int type = rsmd.getColumnType(i + 1);
            if (type == Types.CHAR || type == Types.VARCHAR
                    || type == Types.LONGVARCHAR) {// 在oracle数据库中长文本是clob类型
                typeList.put(column[i], "string");
            } else if (type == Types.NUMERIC || type == Types.INTEGER) {
                typeList.put(column[i], "number");
            } else if (type == Types.DATE || type == Types.TIME
                    || type == Types.TIMESTAMP) {
                typeList.put(column[i], "date");
            } else if (type == Types.BOOLEAN) {
                typeList.put(column[i], "boolean");
            } else if (type == Types.BLOB) {
                typeList.put(column[i], "blob");
            } else if (type == Types.CLOB) {
                typeList.put(column[i], "clob");
            } else {// 处理其他特殊类型列
                typeList.put(column[i], "null");
            }
        }
        return typeList;
    }

    private ArrayList<HashMap<String, Object>> executeSelectSQL(String sql, ArrayList<Object> para)
            throws InvalidResultSetAccessException, Exception, SQLException {

        final ArrayList<Object> exePara = para;

        PreparedStatementSetter pss = new PreparedStatementSetter() {
            public void setValues(PreparedStatement pstmt) throws SQLException {
                Object[] paras = exePara.toArray();
                try {
                    setParas(pstmt, paras);
                } catch (Exception e) {
                    throw new SQLException();
                }
            }
        };

        ResultSetExtractor<ArrayList<HashMap<String, Object>>> resultSetExtractor = new ResultSetExtractor<ArrayList<HashMap<String, Object>>>() {
            public ArrayList<HashMap<String, Object>> extractData(ResultSet rs) throws SQLException,
                    DataAccessException {
                ResultSetMetaData rsmd = rs.getMetaData();
                int columnCount = rsmd.getColumnCount();
                ArrayList<HashMap<String, Object>> ds = new ArrayList<HashMap<String, Object>>();
                try {
                    while (rs.next()) {
                        HashMap<String, Object> rowObj = new HashMap<String, Object>();
                        for (int j = 0; j < columnCount; j++) {
                            String columnName = rsmd.getColumnName(j + 1);
                            Object a = rs.getObject(j + 1);
                            rowObj.put(columnName, a);
                            // 对rowid进行特殊处理
//                            if ("rowid".equals(columnName)) {// oracle
//                                Object rowId = rs.getObject(j + 1);
//                                if (rowId instanceof oracle.sql.ROWID) {
//                                    oracle.sql.ROWID nc = (oracle.sql.ROWID) rowId;
//                                    ds.put(row, columnName, nc.stringValue());
//                                } else {
//                                    ds.put(row, columnName, rs.getString(j + 1));
//                                }
//                            } else {
//                                if (rsmd.getColumnType(j + 1) == Types.LONGVARCHAR) {
//                                    String longVarCharValue = String.valueOf(rs
//                                            .getByte(j + 1));
//                                    ds.put(row, columnName, longVarCharValue);
//                                } else if (rsmd.getColumnType(j + 1) == Types.TIMESTAMP) {
//                                    /**
//                                     * TIMESTAMP类型数据，从oracle中查询出的结果： 1. 可能为
//                                     * oracle.sql.TIMESTAMP 2. 可能为 java.sql.TIMESTAMP
//                                     * 这里统一把2中TIMESTAMP都转换为java.sql.TIMESTAMP
//                                     */
//                                    Object objValue = rs.getObject(j + 1);
//                                    if (objValue == null || objValue instanceof Timestamp) {
//                                        ds.put(row, columnName, objValue);
//                                    } else if (objValue instanceof oracle.sql.TIMESTAMP) {
//                                        oracle.sql.TIMESTAMP timeValue = (oracle.sql.TIMESTAMP) objValue;
//                                        ds.put(row, columnName,
//                                                timeValue.timestampValue());
//                                    } else {
//                                        throw new Exception(
//                                                "在往ArrayList<HashMap<String, Object>>中存储TIMESTAMP类型数据时，从SQL中得到了不可识别的数据类型【" + objValue.getClass().getName() + "】!");
//                                    }
//                                } else {
//                                    Object a = rs.getObject(j + 1);
//                                    ds.put(row, columnName, a);
//                                }
//                            }
                        }
                        ds.add(rowObj);
                    }
                } catch (Exception e) {
                    throw new SQLException();
                }
                return ds;
            }
        };
        jdbcTemplate.setFetchSize(JDBC_FETCH_SIZE);
        ArrayList<HashMap<String, Object>> ds = jdbcTemplate.query(sql, pss, resultSetExtractor);
        assert ds != null;
        if (ds.size() > MAX_ROW_CAPACITY) {
            throw new Exception("【SQL获取的数据超过" + MAX_ROW_CAPACITY + "，总行数为["
                    + ds.size() + "]】SQL:【" + getSql() + "】.");
        }
        return ds;
    }


    /**
     * 获取组装完成的sqlString
     */
    public String getSqlString() throws Exception {
        Object[] args = new Object[para.size()];

        for (int i = 0; i < para.size(); i++) {
            Object o = this.para.get(i);
            if (o instanceof java.lang.Integer) {
                args[i] = ((Integer) o).toString();
            } else if (o instanceof java.lang.Double) {
                args[i] = ((Double) o).toString();
            } else if (o instanceof java.lang.Boolean) {
                args[i] = ((Boolean) o).toString();
            } else if (o instanceof java.lang.String) {
                args[i] = "'" + ((String) o).replaceAll("'", "''") + "'";
            } else if (o instanceof java.sql.Timestamp) {
                args[i] = "to_date('"
                        + DateUtil.FormatDate((java.util.Date) o, "yyyyMMddHHmmss")
                        + "','yyyymmddhh24miss')";
            } else if (o instanceof java.util.Date) {
                args[i] = "to_date('"
                        + DateUtil.FormatDate((java.util.Date) o, "yyyyMMdd")
                        + "','yyyymmdd')";
            } else if (o instanceof java.sql.Blob) {
                throw new Exception("第" + (i + 1) + "个参数类型是Blob，不能转成String");
            } else if (o instanceof BlobValue) {
                throw new Exception("第" + (i + 1) + "个参数类型是BlobValue，不能转成String");
            } else if (o instanceof StringReader) {// 增加对longVarChar类型数据的处理
                throw new Exception("第" + (i + 1) + "个参数类型是LongVarChar，不能转成String");
            } else if (o instanceof BigDecimal) {
                args[i] = ((BigDecimal) o).toString();
            } else if (o instanceof NullValue) {
                args[i] = "null";
            } else if (o == null) {
                throw new Exception("第" + (i + 1) + "个参数未定义");
            } else {
                throw new Exception("第" + (i + 1) + "个参数类型不合法");
            }
        }

        // | var |  str |
        // select 'aa?aa'||?||'bb?bb' a FROM scott.emp
        //        ^     ^
        //		  |		|
        // 		  |	    |
        //        |     |
        //  beginQuoteMarkPos
        //              |
        //		  endQuoteMarkPos
        //
        // 以偶数个单引号为分割点进行分割：
        //
        //		1) - "select 'aa?aa'"
        // 		2) - "||?||'bb?bb'"
        //		3) - " a FROM scott.emp"
        //
        // 每一个分割片段中，单引号之间的字符全部忽略，
        // 单引号之外的"?"替换为对应的变量
        //
        String rawSqlStr = sqlString;
        StringBuilder resultSqlStr = new StringBuilder();

        int quoteMarkScanPos = 0; // 单引号扫描位置
        int beginQuoteMarkPos = -1;
        int endQuoteMarkPos = -1;
        int varReplaceNum = 0;
        while (rawSqlStr.indexOf("'", quoteMarkScanPos) != -1) {
            beginQuoteMarkPos = rawSqlStr.indexOf("'", quoteMarkScanPos);
            endQuoteMarkPos = rawSqlStr.indexOf("'", beginQuoteMarkPos + 1);

            String varQuestionMarksStr = rawSqlStr.substring(quoteMarkScanPos, beginQuoteMarkPos);
            String strQuestionMarksStr = rawSqlStr.substring(beginQuoteMarkPos, endQuoteMarkPos + 1);
            int questionMarkScanPos = 0;
            int questionMarkPos = -1;

            while (varQuestionMarksStr.indexOf("?", questionMarkScanPos) != -1) {
                questionMarkPos = varQuestionMarksStr.indexOf("?", questionMarkScanPos);

                resultSqlStr.append(varQuestionMarksStr.substring(questionMarkScanPos, questionMarkPos)).append((String) args[varReplaceNum++]);

                questionMarkScanPos = questionMarkPos + 1;
            }

            resultSqlStr.append(varQuestionMarksStr.substring(questionMarkScanPos)).append(strQuestionMarksStr);

            quoteMarkScanPos = endQuoteMarkPos + 1;
        }


        // quoteMarkScanPos位置后面的字符串已经不包含【'】符号，单纯解析【?】就可以
        String varQuestionMarksStr = rawSqlStr.substring(quoteMarkScanPos);
        int questionMarkScanPos = 0;
        int questionMarkPos = -1;
        while (varQuestionMarksStr.indexOf("?", questionMarkScanPos) != -1) {
            questionMarkPos = varQuestionMarksStr.indexOf("?", questionMarkScanPos);

            resultSqlStr.append(varQuestionMarksStr.substring(questionMarkScanPos, questionMarkPos)).append((String) args[varReplaceNum++]);

            questionMarkScanPos = questionMarkPos + 1;
        }
        resultSqlStr.append(varQuestionMarksStr.substring(questionMarkScanPos));

        return resultSqlStr.toString();
    }

    public String[] getBatchSqlString() throws Exception {
        if (this.batchParaList == null) {
            return new String[0];
        }

        String[] batchSqlString = new String[this.batchParaList.size()];

        for (int j = 0; j < this.batchParaList.size(); j++) {
            Object[] paras = this.batchParaList.get(j);

            Object[] args = new Object[paras.length];

            for (int i = 0; i < paras.length; i++) {
                Object o = paras[i];
                if (o instanceof java.lang.Integer) {
                    args[i] = ((Integer) o).toString();
                } else if (o instanceof java.lang.Double) {
                    args[i] = ((Double) o).toString();
                } else if (o instanceof java.lang.Boolean) {
                    args[i] = ((Boolean) o).toString();
                } else if (o instanceof java.lang.String) {
                    args[i] = "'" + ((String) o).replaceAll("'", "''") + "'";
                } else if (o instanceof java.sql.Timestamp) {
                    args[i] = "to_date('"
                            + DateUtil.FormatDate((java.util.Date) o, "yyyyMMddHHmmss")
                            + "','yyyymmddhh24miss')";
                } else if (o instanceof java.util.Date) {
                    args[i] = "to_date('"
                            + DateUtil.FormatDate((java.util.Date) o, "yyyyMMdd")
                            + "','yyyymmdd')";
                } else if (o instanceof java.sql.Blob) {
                    throw new Exception("第" + (i + 1) + "个参数类型是Blob，不能转成String");
                } else if (o instanceof BlobValue) {
                    throw new Exception("第" + (i + 1) + "个参数类型是BlobValue，不能转成String");
                } else if (o instanceof StringReader) {// 增加对longVarChar类型数据的处理
                    throw new Exception("第" + (i + 1) + "个参数类型是LongVarChar，不能转成String");
                } else if (o instanceof BigDecimal) {
                    args[i] = ((BigDecimal) o).toString();
                } else if (o instanceof NullValue) {
                    args[i] = "null";
                } else if (o == null) {
                    throw new Exception("第" + (i + 1) + "个参数未定义");
                } else {
                    throw new Exception("第" + (i + 1) + "个参数类型不合法");
                }
            }

            String rawSqlStr = sqlString;
            StringBuilder resultSqlStr = new StringBuilder();

            int quoteMarkScanPos = 0; // 单引号扫描位置
            int beginQuoteMarkPos = -1;
            int endQuoteMarkPos = -1;
            int varReplaceNum = 0;
            while (rawSqlStr.indexOf("'", quoteMarkScanPos) != -1) {
                beginQuoteMarkPos = rawSqlStr.indexOf("'", quoteMarkScanPos);
                endQuoteMarkPos = rawSqlStr.indexOf("'", beginQuoteMarkPos + 1);

                String varQuestionMarksStr = rawSqlStr.substring(quoteMarkScanPos, beginQuoteMarkPos);
                String strQuestionMarksStr = rawSqlStr.substring(beginQuoteMarkPos, endQuoteMarkPos + 1);
                int questionMarkScanPos = 0;
                int questionMarkPos = -1;

                while (varQuestionMarksStr.indexOf("?", questionMarkScanPos) != -1) {
                    questionMarkPos = varQuestionMarksStr.indexOf("?", questionMarkScanPos);

                    resultSqlStr.append(varQuestionMarksStr.substring(questionMarkScanPos, questionMarkPos)).append((String) args[varReplaceNum++]);

                    questionMarkScanPos = questionMarkPos + 1;
                }

                resultSqlStr.append(varQuestionMarksStr.substring(questionMarkScanPos)).append(strQuestionMarksStr);

                quoteMarkScanPos = endQuoteMarkPos + 1;
            }


            String varQuestionMarksStr = rawSqlStr.substring(quoteMarkScanPos);
            int questionMarkScanPos = 0;
            int questionMarkPos = -1;
            while (varQuestionMarksStr.indexOf("?", questionMarkScanPos) != -1) {
                questionMarkPos = varQuestionMarksStr.indexOf("?", questionMarkScanPos);

                resultSqlStr.append(varQuestionMarksStr.substring(questionMarkScanPos, questionMarkPos)).append((String) args[varReplaceNum++]);

                questionMarkScanPos = questionMarkPos + 1;
            }
            resultSqlStr.append(varQuestionMarksStr.substring(questionMarkScanPos));
            batchSqlString[j] = resultSqlStr.toString();
        }
        return batchSqlString;
    }
}
