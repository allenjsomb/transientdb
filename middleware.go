package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

func TokenCheck(authToken string) gin.HandlerFunc {
	return func(c *gin.Context) {
		token := c.Request.Header.Get("x-auth-token")
		if authToken == token {
			c.Next()
		} else {
			log.Info(http.StatusUnauthorized)
			c.AbortWithStatus(http.StatusUnauthorized)
		}
	}
}
