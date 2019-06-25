/*
 Navicat Premium Data Transfer

 Source Server         : xxxxxxxxxxxxxxxxxxxx
 Source Server Type    : MySQL
 Source Server Version : 50724
 Source Host           : xxxxxxxxxxxxxxxxxxxx
 Source Schema         : common_datax

 Target Server Type    : MySQL
 Target Server Version : 50724
 File Encoding         : 65001

 Date: 04/06/2019 21:28:53
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for datax_sync
-- ----------------------------
DROP TABLE IF EXISTS `datax_sync`;
CREATE TABLE `datax_sync`  (
  `SYNC_ID` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '表ID，主键，供其他表做外键',
  `SYNC_NAME` varchar(80) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '同步名称',
  `SYNC_DESCRIPTION` varchar(240) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '同步描述',
  `SOURCE_DATASOURCE_TYPE` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '来源数据源类型，快码：HDSP.DATASOURCE_TYPE',
  `SOURCE_DATASOURCE_ID` bigint(20) NULL DEFAULT NULL COMMENT '来源数据源ID,关联HDSP_CORE_DATASOURCE.DATASOURCE_ID',
  `WRITE_DATASOURCE_TYPE` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '写入数据源类型，快码：HDSP.DATASOURCE_TYPE',
  `WRITE_DATASOURCE_ID` bigint(20) NULL DEFAULT NULL COMMENT '写入数据源ID,关联HDSP_CORE_DATASOURCE.DATASOURCE_ID',
  `JSON_FILE_NAME` varchar(80) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
  `SETTING_INFO` blob NULL COMMENT '数据配置信息',
  `TENANT_ID` bigint(20) NULL DEFAULT NULL COMMENT '租户ID',
  `OBJECT_VERSION_NUMBER` bigint(20) NOT NULL DEFAULT 1 COMMENT '版本号',
  `CREATION_DATE` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  `CREATED_BY` int(11) NOT NULL DEFAULT -1,
  `LAST_UPDATED_BY` int(11) NOT NULL DEFAULT -1,
  `LAST_UPDATE_DATE` datetime(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  PRIMARY KEY (`SYNC_ID`) USING BTREE,
  UNIQUE INDEX `SYNC_ID`(`SYNC_ID`) USING BTREE,
  UNIQUE INDEX `SYNC_NAME`(`SYNC_NAME`) USING BTREE,
  UNIQUE INDEX `JSON_FILE_NAME`(`JSON_FILE_NAME`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 8 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_bin COMMENT = '数据同步表' ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
