package com.wqy.springbootes.service;

import com.wqy.springbootes.SpringbootEsApplicationTests;
import com.wqy.springbootes.service.house.IAddressService;
import com.wqy.springbootes.service.search.BaiduMapLocation;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class AddressServiceTest extends SpringbootEsApplicationTests {

    @Autowired
    private IAddressService addressService;

    @Test
    public void test(){
        String city = "北京";
        String address = "北京市昌平区巩华家园1号楼2单元";
        ServiceResult<BaiduMapLocation> mapLocation = addressService.getBaiduMapLocation(city,address);
        Assert.assertTrue(mapLocation.getResult().getLatitude()>0);

    }
}
