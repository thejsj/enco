package imageConverter

import (
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/gographics/imagick/imagick"
)

func Resize(fileName string) (resizeError error) {
	imagick.Initialize()
	// Schedule cleanup
	defer imagick.Terminate()
	var err error

	mw := imagick.NewMagickWand()
	// Schedule cleanup
	defer mw.Destroy()

	err = mw.ReadImage(fileName)
	if err != nil {
		return err
	}

	// Get original logo size
	width := mw.GetImageWidth()
	height := mw.GetImageHeight()
	log.Printf("With: %v / Height: %v", width, height)

	// Calculate half the size
	hWidth := uint(width / 2)
	hHeight := uint(height / 2)

	// Resize the image using the Lanczos filter
	// The blur factor is a float, where > 1 is blurry, < 1 is sharp
	err = mw.ResizeImage(hWidth, hHeight, imagick.FILTER_LANCZOS, 1)
	if err != nil {
		log.Printf("Error resizing image: %v", err)
		return err
	}

	// Set the compression quality to 95 (high quality = low compression)
	err = mw.SetImageCompressionQuality(95)
	if err != nil {
		log.Printf("Error setting compression quaility: %v", err)
		return err
	}
	fileExtension := filepath.Ext(fileName)
	name := strings.TrimSuffix(fileName, fileExtension)
	converteImageFileName := name + "-" + string(time.Now().Format(time.RFC850)) + fileExtension

	log.Printf("Starting to convert image: %v", converteImageFileName)
	mw.WriteImage(converteImageFileName)
	log.Printf("Finished converting image: %v", converteImageFileName)
	return nil
}
