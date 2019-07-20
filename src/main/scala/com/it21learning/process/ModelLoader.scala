package com.it21learning.process

import java.util.Date

import com.it21learning.config.settings.ModelSettings
import com.it21learning.config.Settings
import org.apache.spark.ml.PipelineModel

//To load random forest classification mode built during training process
class ModelLoader private() { self: ModelSettings =>
  //last load
  private var tmLastLoad: Option[Date] = None
  //the model last load
  private var rfModel: Option[PipelineModel] = None

  //load the model
  def get(): PipelineModel = {
    //check
    if ( tmLastLoad == None || rfModel == None
      || ((new Date()).getTime() - tmLastLoad.get.getTime()) > this.loadInterval.toMillis ) {
      //set time
      tmLastLoad = Some(new Date())
      //load
      rfModel = Some(PipelineModel.load(this.dir))
    }
    //check
    rfModel match {
      case None => throw new Exception("The model is not available or loading model failed.")
      case Some(model) => model
    }
  }
}

object ModelLoader {
  //constructor
  def apply(): ModelLoader = new ModelLoader() with Settings.Model.Settings
}