package com.wqy.springbootes.web.controller;

import com.wqy.springbootes.base.ApiResponse;
import com.wqy.springbootes.base.LoginUserUtil;
import com.wqy.springbootes.service.ISmsService;
import com.wqy.springbootes.service.ServiceResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class HomeController {

    @Autowired
    private ISmsService smsService;

    @GetMapping(value = {"/", "/index"})
    public String index(Model model) {
        return "index";
    }

    @GetMapping("/404")
    public String notFoundPage() {
        return "404";
    }

    @GetMapping("/403")
    public String accessError() {
        return "403";
    }

    @GetMapping("/500")
    public String internalError() {
        return "500";
    }

    @GetMapping("/logout/page")
    public String logoutPage() {
        return "logout";
    }

    @GetMapping(value = "sms/code")
    @ResponseBody
    public ApiResponse smsCode(@RequestParam("telephone")String telephone){

        if(!LoginUserUtil.checkTelephone(telephone)){
            return ApiResponse.ofMessage(HttpStatus.BAD_REQUEST.value(),"请输入正确手机号");
        }
        ServiceResult<String> result = smsService.sendSms(telephone);
        if(result.isSuccess()){
            return ApiResponse.ofSuccess("");
        }else{
            return ApiResponse.ofMessage(HttpStatus.BAD_REQUEST.value(),result.getMessage());
        }

    }
}
