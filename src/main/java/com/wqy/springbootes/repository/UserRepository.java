package com.wqy.springbootes.repository;

import com.wqy.springbootes.entity.User;
import org.springframework.data.repository.CrudRepository;

public interface UserRepository extends CrudRepository<User,Long> {
}
