from PIL import Image, ImageEnhance, ImageOps, ImageFilter
from deskew import determine_skew
from skimage import io
from skimage.transform import rotate
import numpy as np
import os

def preprocess(image, i):
     #read the image
     im1 = Image.open(image)
     #im1.show()

     ############################
     # Increase Contrast #
     ############################

     #image brightness enhancer
     enhancer = ImageEnhance.Contrast(im1)

     factor = 1.5 #increase contrast
     im2 = enhancer.enhance(factor)

     #im2.show()
     #im2.save('more-contrast-image.png')


     ############################
          # Binarize Image #
     ############################

     # https://stackoverflow.com/questions/68957686/pillow-how-to-binarize-an-image-with-threshold
     #grayscale
     im3 = im2.convert('L')
     #im3 = ImageOps.grayscale(im2)
     #im3.show()

     # Threshold
     threshold = 120
     im4 = im3.point( lambda p: 255 if p > threshold else 0 )

     # To mono
     im5 = im4.convert('1')

     #im5.show()
     im3.save('test1.png')

     ############################
          # Deskew #
     ############################

     image = io.imread('test1.png')
     angle = determine_skew(image)
     rotated = rotate(image, angle, resize=True) * 255
     name = "outputs2/output" + str(i) + ".png"
     io.imsave(name, rotated.astype(np.uint8))


     ############################
          # Other #
     ############################

     #test = im5.filter(ImageFilter.DETAIL)
     #test = test.filter(ImageFilter.SHARPEN)

     #test.show()

     ############################
          # Remove Noise #
     ############################
     # https://stackoverflow.com/questions/63098792/how-to-remove-noise-from-an-image-using-pillow

     #im6 = im5.filter(ImageFilter.BLUR)

     #im7 = im5.filter(ImageFilter.MinFilter(3))
     #im8 = im5.filter(ImageFilter.MinFilter)  # same as im7
     
     #im7.show()

     # https://github.com/imneonizer/Image-Background-Filter-with-Python/blob/master/image_background_filter.py
     #im = im1.filter(ImageFilter.MedianFilter())
     #enhancer = ImageEnhance.Contrast(im)
     #im = enhancer.enhance(1.5)
     #im = im.convert('1')
     #im.show()

def process_all(directory):
     i = 0
     for image in os.listdir(directory):
          #print(image)
          preprocess("cards/" + image, i)
          i += 1

directory = r'/Users/shreyahprasad/ua_pipeline/cards'
#process_all(directory)

#preprocess("testcard.png", '')