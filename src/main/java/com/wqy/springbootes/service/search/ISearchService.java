package com.wqy.springbootes.service.search;

import com.wqy.springbootes.service.ServiceMultiResult;
import com.wqy.springbootes.service.ServiceResult;
import com.wqy.springbootes.web.dto.RentSearch;
import com.wqy.springbootes.web.dto.SupportAddressDTO;
import com.wqy.springbootes.web.form.MapSearch;

import java.util.List;
import java.util.Map;

/**
 * 检索接口
 */
public interface ISearchService {

    /**
     * 索引目标房源
     * @param houseId
     */
    void index(Long houseId);

    /**
     * 移除房源索引
     * @param houseId
     */
    void remove(Long houseId);


    ServiceMultiResult<Long> query(RentSearch rentSearch);

    /**
     * 获取补全建议关键词
     * @param prefix
     * @return
     */
    ServiceResult<List<String>> suggest(String prefix);


    ServiceResult<Long> aggregateDistrictHouse(String cityName,String regionEnName,String district);

    /**
     * 聚合城市数据
     * @return
     */
    ServiceMultiResult<HouseBucketDTO> mapAggregate(String cityEnName);

    /**
     * 城市级别查询
     * @param cityEnName
     * @param orderBy
     * @param orderDirection
     * @param start
     * @param size
     * @return
     */
    ServiceMultiResult<Long> mapQuery(String cityEnName,String orderBy,String orderDirection,int start ,int size);

    /**
     * 精确范围查询
     * @param mapSearch
     * @return
     */
    ServiceMultiResult<Long> mapQuery(MapSearch mapSearch);



}
