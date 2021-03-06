package com.wqy.springbootes.repository;

import java.util.List;

import com.wqy.springbootes.entity.HousePicture;
import org.springframework.data.repository.CrudRepository;


/**
 * Created by 瓦力.
 */
public interface HousePictureRepository extends CrudRepository<HousePicture, Long> {
    List<HousePicture> findAllByHouseId(Long id);
}
