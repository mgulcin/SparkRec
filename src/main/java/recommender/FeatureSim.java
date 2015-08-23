package recommender;

import scala.Serializable;

public class FeatureSim implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -105353900078666273L;
	
	Integer featureId;
	Double cosSim;
	
	
	public FeatureSim(Integer featureId, Double cosSim) {
		super();
		this.featureId = featureId;
		this.cosSim = cosSim;
	}
	
	public Integer getFeatureId() {
		return featureId;
	}
	public void setFeatureId(Integer featureId) {
		this.featureId = featureId;
	}
	public Double getCosSim() {
		return cosSim;
	}
	public void setCosSim(Double cosSim) {
		this.cosSim = cosSim;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		
		if(featureId != null){
			builder.append(featureId);
		} else {
			builder.append("NULL");
		}
		builder.append(" , ");
		if(cosSim != null){
			builder.append(cosSim);
		} else {
			builder.append("NULL");
		}
		return builder.toString();
	}
	
	
}
