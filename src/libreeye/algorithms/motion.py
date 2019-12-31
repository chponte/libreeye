import cv2

cv2.setUseOptimized(True)


def basic_motion(frame_iter, delta, min_area, cooldown, debug):
    # https://github.com/YaoQ/motion-detection-with-opencv/blob/
    # e040c1a77a2545136efb35fa873443b34cde2fa0/motion-detector.py
    # initialize the camera and grab a reference to the raw camera capture
    avg = None
    last_motion = -cooldown
    motion_frames = list()
    # capture frames from the camera
    for frame_num, frame in enumerate(frame_iter()):
        # assume no motion by default
        frame_has_motion = False
        # blur frame
        gray = cv2.GaussianBlur(frame, (21, 21), 0)
        # if the average frame is None, initialize it
        if avg is None:
            avg = gray.copy().astype("float")
            continue
        # accumulate the weighted average between the current and previous 
        cv2.accumulateWeighted(gray, avg, 0.5)
        # check to see if enough time has passed between uploads
        if frame_num - last_motion < cooldown:
            continue
        # compute the difference between the current frame and running average
        frame_delta = cv2.absdiff(gray, cv2.convertScaleAbs(avg))
        # threshold the delta image, dilate the thresholded image to fill
        # in holes, then find contours on thresholded image
        thresh = cv2.threshold(frame_delta, delta, 255, cv2.THRESH_BINARY)[1]
        thresh = cv2.dilate(thresh, None, iterations=2)
        cnts, _ = cv2.findContours(
            thresh,
            cv2.RETR_EXTERNAL,
            cv2.CHAIN_APPROX_SIMPLE
        )
        # loop over the contours
        for c in cnts:
            # if the contour is too small, ignore it
            if cv2.contourArea(c) < min_area:
                continue
            frame_has_motion = True
            # compute the bounding box for the contour, draw it on the frame,
            # and update the text
            (x, y, w, h) = cv2.boundingRect(c)
            cv2.rectangle(frame, (x, y), (x + w, y + h), (0, 255, 0), 2)
        # check to see if there is motion
        if frame_has_motion:
            last_motion = frame_num
            motion_frames.append((frame_num, frame))
        # check to see if the frames should be displayed to screen
        if debug:
            # display the security feed
            cv2.imshow("Window", frame)
            cv2.waitKey(1)
    return motion_frames
