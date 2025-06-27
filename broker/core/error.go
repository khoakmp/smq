package core

import "errors"

var ErrExiting = errors.New("exiting")

var ErrTopicNotExisted = errors.New("topic not existed")
var ErrTopicExisted = errors.New("topic already existed")

var ErrConsumerGroupExisted = errors.New("consumer group already existed")
var ErrConsumerGroupNotExisted = errors.New("consumer group not existed")
