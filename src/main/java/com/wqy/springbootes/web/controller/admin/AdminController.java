package com.wqy.springbootes.web.controller.admin;

import java.io.*;
import java.sql.Blob;
import java.util.Map;


import com.google.common.base.Strings;
import com.sun.org.apache.xml.internal.security.utils.Base64;
import com.wqy.springbootes.base.ApiDataTableResponse;
import com.wqy.springbootes.base.ApiResponse;
import com.wqy.springbootes.base.HouseOperation;
import com.wqy.springbootes.base.HouseStatus;
import com.wqy.springbootes.entity.House;
import com.wqy.springbootes.entity.HousePicture;
import com.wqy.springbootes.entity.SubwayStation;
import com.wqy.springbootes.entity.SupportAddress;
import com.wqy.springbootes.service.IUserService;
import com.wqy.springbootes.service.ServiceMultiResult;
import com.wqy.springbootes.service.ServiceResult;
import com.wqy.springbootes.service.house.IAddressService;
import com.wqy.springbootes.service.house.IHouseService;
import com.wqy.springbootes.web.dto.*;
import com.wqy.springbootes.web.form.DatatableSearch;
import com.wqy.springbootes.web.form.HouseForm;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

//import com.google.gson.Gson;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.rowset.serial.SerialBlob;
import javax.validation.Valid;

/**
 * Created by 瓦力.
 */
@Controller
public class AdminController {


    //@Autowired
    //private Gson gson;

    @Autowired
    private IAddressService addressService;

    @Autowired
    private IHouseService houseService;

    @Autowired
    private IUserService userService;

    /**
     * 后台管理中心
     * @return
     */
    @GetMapping("/admin/center")
    public String adminCenterPage() {
        return "admin/center";
    }

    /**
     * 欢迎页
     * @return
     */
    @GetMapping("/admin/welcome")
    public String welcomePage() {
        return "admin/welcome";
    }

    /**
     * 管理员登录页
     * @return
     */
    @GetMapping("/admin/login")
    public String adminLoginPage() {
        return "admin/login";
    }

    /**
     * 房源列表页
     * @return
     */
    @GetMapping("admin/house/list")
    public String houseListPage() {
        return "admin/house-list";
    }

    /**
     * 新增房源功能页
     * @return
     */
    @GetMapping("/admin/add/house")
    public String addHousePage() {
        return "admin/house-add";
    }


    @PostMapping("admin/houses")
    @ResponseBody
    public ApiDataTableResponse houses(@ModelAttribute DatatableSearch searchBody) {
        ServiceMultiResult<HouseDTO> result = houseService.adminQuery(searchBody);

        ApiDataTableResponse response = new ApiDataTableResponse(ApiResponse.Status.SUCCESS);
        response.setData(result.getResult());
        response.setRecordsFiltered(result.getTotal());
        response.setRecordsTotal(result.getTotal());

        response.setDraw(searchBody.getDraw());
        return response;
    }

    /**
     * 上传图片接口
     * @param file
     * @return
     */
    @PostMapping(value = "/admin/upload/photo", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @ResponseBody
    public ApiResponse uploadPhoto(@RequestParam("file") MultipartFile file) {
        if (file.isEmpty()) {
            return ApiResponse.ofStatus(ApiResponse.Status.NOT_VALID_PARAM);
        }

        String fileName = file.getOriginalFilename();
        File target = new File("D:/APP/SpingBoot/uploadFile_dir/springboot-es/"+fileName);
        try {
            file.transferTo(target);
            InputStream inputStream = file.getInputStream();
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] b = new byte[1024];
            int n;
            byte[] buffer = null;
            while ((n = inputStream.read(b)) != -1){
                bos.write(b, 0, n);
            }
            inputStream.close();
            bos.close();
            buffer = bos.toByteArray();
            HousePicture house=new HousePicture();
            house.setImg(new BASE64Encoder().encode(buffer));
            ServiceResult<HousePicture> result = houseService.save(house);
            if (result.isSuccess()) {
                return ApiResponse.ofSuccess(result.getResult());
            }
            /*Response response = qiNiuervice.uploadFile(inputStream);
            if (response.isOK()) {
                QiNiuPutRet ret = gson.fromJson(response.bodyString(), QiNiuPutRet.class);
                return ApiResponse.ofSuccess(ret);
            } else {S
                return ApiResponse.ofMessage(response.statusCode, response.getInfo());
            }*/
            return ApiResponse.ofSuccess(null);
        } catch (QiniuException e) {
            Response response = e.response;
            try {
                return ApiResponse.ofMessage(response.statusCode, response.bodyString());
            } catch (QiniuException e1) {
                e1.printStackTrace();
                return ApiResponse.ofStatus(ApiResponse.Status.INTERNAL_SERVER_ERROR);
            }
        } catch (IOException e) {
            return ApiResponse.ofStatus(ApiResponse.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/showImg")
    public void showimg(HttpServletRequest request, HttpServletResponse response) throws Exception {
        ServiceResult<HousePicture> pictureServiceResult = houseService.showPhoto(86L);
        HousePicture housePicture = pictureServiceResult.getResult();
        String img= housePicture.getImg();
        byte[] bytes = img.getBytes();

        byte[] rs =new BASE64Decoder().decodeBuffer(img);
        /*Blob b = new SerialBlob(img.getBytes("GBK"));

        int length = (int) b.length();
        byte[] bImage = new byte[length];
        InputStream is = new BufferedInputStream(b.getBinaryStream());
        is.read(bImage, 0, length);*/
        OutputStream out = response.getOutputStream();
        out.write(rs);
        //out.flush();
       // out.close();
        //is.close();
    }
    /**
     * 新增房源接口
     * @param houseForm
     * @param bindingResult
     * @return
     */
    @PostMapping("admin/add/house")
    @ResponseBody
    public ApiResponse addHouse(@Valid @ModelAttribute("form-house-add") HouseForm houseForm, BindingResult bindingResult) {
        if (bindingResult.hasErrors()) {
            return new ApiResponse(HttpStatus.BAD_REQUEST.value(), bindingResult.getAllErrors().get(0).getDefaultMessage(), null);
        }

        if (houseForm.getPhotos() == null || houseForm.getCover() == null) {
            return ApiResponse.ofMessage(HttpStatus.BAD_REQUEST.value(), "必须上传图片");
        }

        Map<SupportAddress.Level, SupportAddressDTO> addressMap = addressService.findCityAndRegion(houseForm.getCityEnName(), houseForm.getRegionEnName());
        if (addressMap.keySet().size() != 2) {
            return ApiResponse.ofStatus(ApiResponse.Status.NOT_VALID_PARAM);
        }

        ServiceResult<HouseDTO> result = houseService.save(houseForm);
        if (result.isSuccess()) {
            return ApiResponse.ofSuccess(result.getResult());
        }

        return ApiResponse.ofSuccess(ApiResponse.Status.NOT_VALID_PARAM);
    }


    /**
     * 编辑页面
     * @param id
     * @param model
     * @return
     */
    @GetMapping("admin/house/edit")
    public String houseEditPage(@RequestParam(value = "id")Long id,Model model){

        if(id ==null ||id <1){
            return "404";
        }
        ServiceResult<HouseDTO> serviceResult = houseService.findCompleteOne(id);
        if(!serviceResult.isSuccess()){
            return "404";
        }

        HouseDTO result = serviceResult.getResult();
        model.addAttribute("house",result);

        Map<SupportAddress.Level,SupportAddressDTO> addressDTOMap = addressService.
                findCityAndRegion(result.getCityEnName(),result.getRegionEnName());
        model.addAttribute("city",addressDTOMap.get(SupportAddress.Level.CITY));
        model.addAttribute("region",addressDTOMap.get(SupportAddress.Level.REGION));

        HouseDetailDTO detailDTO = result.getHouseDetail();
        ServiceResult<SubwayDTO> subwayDTOServiceResult = addressService.findSubway(detailDTO.getSubwayLineId());
        if(subwayDTOServiceResult.isSuccess()){
            model.addAttribute("subway",subwayDTOServiceResult.getResult());
        }

        ServiceResult<SubwayStationDTO> subwayStationDTOServiceResult = addressService.findSubwayStation(detailDTO.getSubwayLineId());
        if(subwayDTOServiceResult.isSuccess()){
            model.addAttribute("station",subwayStationDTOServiceResult.getResult());
        }

        return "admin/house-edit";
    }

    /**
     * 编辑房源信息
     * @param houseForm
     * @param bindingResult
     * @return
     */
    @PostMapping("admin/house/edit")
    @ResponseBody
    public ApiResponse saveHouse(@Valid @ModelAttribute("form-house-edit") HouseForm houseForm, BindingResult bindingResult){
        if (bindingResult.hasErrors()) {
            return new ApiResponse(HttpStatus.BAD_REQUEST.value(), bindingResult.getAllErrors().get(0).getDefaultMessage(), null);
        }

        Map<SupportAddress.Level, SupportAddressDTO> addressMap = addressService.findCityAndRegion(houseForm.getCityEnName(), houseForm.getRegionEnName());
        if (addressMap.keySet().size() != 2) {
            return ApiResponse.ofStatus(ApiResponse.Status.NOT_VALID_PARAM);
        }

        ServiceResult<HouseDTO> result = houseService.update(houseForm);
        if (result.isSuccess()) {
            return ApiResponse.ofSuccess(null);
        }
        ApiResponse apiResponse = ApiResponse.ofStatus(ApiResponse.Status.BAD_REQUEST);
        apiResponse.setMessage(result.getMessage());
        return apiResponse;
    }


    @PutMapping("admin/house/operate/{id}/{operation}")
    @ResponseBody
    public ApiResponse auditHouse(@PathVariable(value = "id")Long id,
                                  @PathVariable(value = "operation") int operation){

        if(id <=0){
            return ApiResponse.ofStatus(ApiResponse.Status.NOT_VALID_PARAM);
        }
        ServiceResult result;
        switch (operation){
            case HouseOperation.PASS:
                result = this.houseService.updateStatus(id,HouseStatus.PASSES.getValue());
                break;
            case HouseOperation.PULL_OUT:
                result =this.houseService.updateStatus(id,HouseStatus.NOT_AUDITED.getValue());
                break;
            case HouseOperation.RENT:
                result = this.houseService.updateStatus(id,HouseStatus.RENTED.getValue());
                break;
            default:
                return ApiResponse.ofStatus(ApiResponse.Status.BAD_REQUEST);
        }
        if(result.isSuccess()){
            return ApiResponse.ofSuccess(null);
        }

        return ApiResponse.ofMessage(ApiResponse.Status.BAD_REQUEST.getCode(),result.getMessage());

    }

    /**
     * 移除图片接口
     * @param id
     * @return
     */
    @DeleteMapping("admin/house/photo")
    @ResponseBody
    public ApiResponse removeHousePhoto(@RequestParam(value = "id") Long id) {
        ServiceResult result = this.houseService.removePhoto(id);

        if (result.isSuccess()) {
            return ApiResponse.ofStatus(ApiResponse.Status.SUCCESS);
        } else {
            return ApiResponse.ofMessage(HttpStatus.BAD_REQUEST.value(), result.getMessage());
        }
    }

    /**
     * 修改封面接口
     * @param coverId
     * @param targetId
     * @return
     */
    @PostMapping("admin/house/cover")
    @ResponseBody
    public ApiResponse updateCover(@RequestParam(value = "cover_id") Long coverId,
                                   @RequestParam(value = "target_id") Long targetId) {
        ServiceResult result = this.houseService.updateCover(coverId, targetId);

        if (result.isSuccess()) {
            return ApiResponse.ofStatus(ApiResponse.Status.SUCCESS);
        } else {
            return ApiResponse.ofMessage(HttpStatus.BAD_REQUEST.value(), result.getMessage());
        }
    }

    /**
     * 增加标签接口
     * @param houseId
     * @param tag
     * @return
     */
    @PostMapping("admin/house/tag")
    @ResponseBody
    public ApiResponse addHouseTag(@RequestParam(value = "house_id") Long houseId,
                                   @RequestParam(value = "tag") String tag) {
        if (houseId < 1 || Strings.isNullOrEmpty(tag)) {
            return ApiResponse.ofStatus(ApiResponse.Status.BAD_REQUEST);
        }

        ServiceResult result = this.houseService.addTag(houseId, tag);
        if (result.isSuccess()) {
            return ApiResponse.ofStatus(ApiResponse.Status.SUCCESS);
        } else {
            return ApiResponse.ofMessage(HttpStatus.BAD_REQUEST.value(), result.getMessage());
        }
    }

    /**
     * 移除标签接口
     * @param houseId
     * @param tag
     * @return
     */
    @DeleteMapping("admin/house/tag")
    @ResponseBody
    public ApiResponse removeHouseTag(@RequestParam(value = "house_id") Long houseId,
                                      @RequestParam(value = "tag") String tag) {
        if (houseId < 1 || Strings.isNullOrEmpty(tag)) {
            return ApiResponse.ofStatus(ApiResponse.Status.BAD_REQUEST);
        }

        ServiceResult result = this.houseService.removeTag(houseId, tag);
        if (result.isSuccess()) {
            return ApiResponse.ofStatus(ApiResponse.Status.SUCCESS);
        } else {
            return ApiResponse.ofMessage(HttpStatus.BAD_REQUEST.value(), result.getMessage());
        }
    }



    @GetMapping("admin/house/subscribe")
    public String houseSubscribe() {
        return "admin/subscribe";
    }

    @GetMapping("admin/house/subscribe/list")
    @ResponseBody
    public ApiResponse subscribeList(@RequestParam(value = "draw") int draw,
                                     @RequestParam(value = "start") int start,
                                     @RequestParam(value = "length") int size) {
        ServiceMultiResult<Pair<HouseDTO, HouseSubscribeDTO>> result = houseService.findSubscribeList(start, size);

        ApiDataTableResponse response = new ApiDataTableResponse(ApiResponse.Status.SUCCESS);
        response.setData(result.getResult());
        response.setDraw(draw);
        response.setRecordsFiltered(result.getTotal());
        response.setRecordsTotal(result.getTotal());
        return response;
    }

    @GetMapping("admin/user/{userId}")
    @ResponseBody
    public ApiResponse getUserInfo(@PathVariable(value = "userId")Long userId){
        if(userId==null||userId<1){
            return ApiResponse.ofStatus(ApiResponse.Status.BAD_REQUEST);
        }

        ServiceResult<UserDTO> serviceResult = userService.findById(userId);
        if(!serviceResult.isSuccess()){
            return ApiResponse.ofStatus(ApiResponse.Status.NOT_FOUND);
        }else {
            return ApiResponse.ofSuccess(serviceResult.getResult());
        }
    }

    @PostMapping("admin/finish/subscribe")
    @ResponseBody
    public ApiResponse finishSubscribe(@RequestParam(value = "house_id")Long houseId){
        ServiceResult serviceResult = houseService.finishSubscribe(houseId);
        if(!serviceResult.isSuccess()){
            return ApiResponse.ofMessage(ApiResponse.Status.BAD_REQUEST.getCode(),serviceResult.getMessage());
        }else {
            return ApiResponse.ofSuccess("");
        }
    }



















}

